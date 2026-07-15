package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.FeatureType;
import com.fiberhome.ml.raha.feature.FeatureDefinition;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.model.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.ColumnModelStore;
import com.fiberhome.ml.raha.util.FormDataCodec;
import com.fiberhome.ml.raha.util.ValueUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使用 FMDB 表保存不可变列级模型和特征字典，并按版本缓存加载结果。
 */
public final class FmdbModelStore implements ColumnModelStore {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FmdbModelStore.class);
    /** FMDB 模型路径协议。 */
    private static final String MODEL_SCHEME = "fmdb://";
    /** 空特征字典使用的稳定占位编号。 */
    private static final int EMPTY_DICTIONARY_INDEX = -1;
    /** 模型参数表模式。 */
    private static final StructType MODEL_SCHEMA = DataTypes.createStructType(
            new StructField[]{
                    field("model_version", DataTypes.StringType, false),
                    field("model_name", DataTypes.StringType, false),
                    field("column_name", DataTypes.StringType, false),
                    field("classifier_type", DataTypes.StringType, false),
                    field("dictionary_version", DataTypes.StringType, false),
                    field("feature_dimension", DataTypes.IntegerType, false),
                    field("threshold", DataTypes.DoubleType, false),
                    field("intercept", DataTypes.DoubleType, false),
                    field("coefficients", DataTypes.StringType, false),
                    field("training_mode", DataTypes.StringType, false),
                    field("stored_at", DataTypes.LongType, false)
            });
    /** 特征字典表模式。 */
    private static final StructType DICTIONARY_SCHEMA = DataTypes.createStructType(
            new StructField[]{
                    field("dictionary_version", DataTypes.StringType, false),
                    field("column_name", DataTypes.StringType, false),
                    field("created_at", DataTypes.LongType, false),
                    field("feature_index", DataTypes.IntegerType, false),
                    field("feature_name", DataTypes.StringType, true),
                    field("feature_type", DataTypes.StringType, true),
                    field("feature_source", DataTypes.StringType, true),
                    field("default_value", DataTypes.DoubleType, true),
                    field("stored_at", DataTypes.LongType, false)
            });
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 模型参数表名。 */
    private final String modelTable;
    /** 特征字典表名。 */
    private final String dictionaryTable;
    /** 提供可测试存储时间的时钟。 */
    private final Clock clock;
    /** 按完整路径缓存已加载模型。 */
    private final Map<String, ColumnModelArtifact> modelCache =
            new ConcurrentHashMap<String, ColumnModelArtifact>();
    /** 按字典版本缓存已加载字典。 */
    private final Map<String, FeatureDictionary> dictionaryCache =
            new ConcurrentHashMap<String, FeatureDictionary>();

    public FmdbModelStore(SparkSession sparkSession,
                          FmdbTableGateway tableGateway,
                          String modelTable,
                          String dictionaryTable,
                          Clock clock) {
        if (sparkSession == null || tableGateway == null || clock == null) {
            throw new IllegalArgumentException("FMDB 模型存储依赖不能为空");
        }
        this.modelTable = SparkSqlFmdbTableGateway.validateTableName(modelTable);
        this.dictionaryTable = SparkSqlFmdbTableGateway.validateTableName(dictionaryTable);
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.clock = clock;
    }

    @Override
    public synchronized String save(ColumnModelArtifact artifact) {
        if (artifact == null) {
            throw new IllegalArgumentException("FMDB 模型参数不能为空");
        }
        String path = modelPath(artifact.getModelVersion());
        LOGGER.info("开始保存 FMDB 列级模型，modelVersion={}，columnName={}",
                artifact.getModelVersion(), artifact.getColumnName());
        ColumnModelArtifact existing = findModel(artifact.getModelVersion());
        // 同一模型版本必须对应完全相同的不可变参数，防止版本污染。
        if (existing != null) {
            if (!sameModel(existing, artifact)) {
                throw new IllegalStateException("FMDB 中同一模型版本已存在不同参数");
            }
            modelCache.put(path, existing);
            return path;
        }
        Map<String, String> coefficients = new LinkedHashMap<String, String>();
        for (Map.Entry<Integer, Double> entry : artifact.getCoefficients().entrySet()) {
            coefficients.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        Row row = RowFactory.create(artifact.getModelVersion(), artifact.getModelName(),
                artifact.getColumnName(), artifact.getClassifierType().name(),
                artifact.getFeatureDictionaryVersion(), artifact.getFeatureDimension(),
                artifact.getThreshold(), artifact.getIntercept(),
                FormDataCodec.encode(coefficients), artifact.getTrainingMode(), positiveNow());
        tableGateway.appendIdempotent(modelTable,
                sparkSession.createDataFrame(Collections.singletonList(row), MODEL_SCHEMA),
                Collections.singletonList("model_version"));
        modelCache.put(path, artifact);
        LOGGER.info("FMDB 列级模型保存完成，modelVersion={}，columnName={}",
                artifact.getModelVersion(), artifact.getColumnName());
        return path;
    }

    @Override
    public ColumnModelArtifact load(String modelPath) {
        String version = versionFromPath(modelPath);
        ColumnModelArtifact cached = modelCache.get(modelPath);
        if (cached != null) {
            return cached;
        }
        LOGGER.info("开始加载 FMDB 列级模型，modelVersion={}", version);
        ColumnModelArtifact artifact = findModel(version);
        if (artifact == null) {
            throw new IllegalStateException("FMDB 中不存在模型版本：" + version);
        }
        modelCache.put(modelPath, artifact);
        LOGGER.info("FMDB 列级模型加载完成，modelVersion={}，columnName={}",
                artifact.getModelVersion(), artifact.getColumnName());
        return artifact;
    }

    /**
     * 幂等保存特征字典，字典版本相同但内容变化时拒绝覆盖。
     */
    public synchronized String saveDictionary(FeatureDictionary dictionary) {
        if (dictionary == null) {
            throw new IllegalArgumentException("FMDB 特征字典不能为空");
        }
        FeatureDictionary existing = findDictionary(dictionary.getVersion());
        if (existing != null) {
            if (!sameDictionary(existing, dictionary)) {
                throw new IllegalStateException("FMDB 中同一字典版本已存在不同内容");
            }
            dictionaryCache.put(dictionary.getVersion(), existing);
            return dictionaryPath(dictionary.getVersion());
        }
        List<Row> rows = new ArrayList<Row>();
        long storedAt = positiveNow();
        if (dictionary.getDefinitions().isEmpty()) {
            rows.add(RowFactory.create(dictionary.getVersion(), dictionary.getColumnName(),
                    dictionary.getCreatedAt(), EMPTY_DICTIONARY_INDEX,
                    null, null, null, null, storedAt));
        } else {
            for (FeatureDefinition definition : dictionary.getDefinitions().values()) {
                rows.add(RowFactory.create(dictionary.getVersion(), dictionary.getColumnName(),
                        dictionary.getCreatedAt(), definition.getIndex(), definition.getName(),
                        definition.getFeatureType().name(), definition.getSource(),
                        definition.getDefaultValue(), storedAt));
            }
        }
        tableGateway.appendIdempotent(dictionaryTable,
                sparkSession.createDataFrame(rows, DICTIONARY_SCHEMA),
                Arrays.asList("dictionary_version", "feature_index"));
        dictionaryCache.put(dictionary.getVersion(), dictionary);
        LOGGER.info("FMDB 特征字典保存完成，dictionaryVersion={}，featureCount={}",
                dictionary.getVersion(), dictionary.getDefinitions().size());
        return dictionaryPath(dictionary.getVersion());
    }

    /**
     * 按不可变版本加载特征字典。
     */
    public FeatureDictionary loadDictionary(String dictionaryVersion) {
        String version = ValueUtils.requireNotBlank(
                dictionaryVersion, "FMDB 特征字典版本");
        FeatureDictionary cached = dictionaryCache.get(version);
        if (cached != null) {
            return cached;
        }
        FeatureDictionary dictionary = findDictionary(version);
        if (dictionary == null) {
            throw new IllegalStateException("FMDB 中不存在特征字典版本：" + version);
        }
        dictionaryCache.put(version, dictionary);
        return dictionary;
    }

    private ColumnModelArtifact findModel(String version) {
        if (!tableGateway.tableExists(modelTable)) {
            return null;
        }
        List<Row> rows = tableGateway.read(modelTable)
                .filter(functions.col("model_version").equalTo(version))
                .limit(2).collectAsList();
        if (rows.isEmpty()) {
            return null;
        }
        if (rows.size() > 1) {
            throw new IllegalStateException("FMDB 模型版本存在重复记录：" + version);
        }
        Row row = rows.get(0);
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        String encoded = row.getAs("coefficients");
        if (encoded != null && !encoded.isEmpty()) {
            for (Map.Entry<String, String> entry : FormDataCodec.decode(encoded).entrySet()) {
                coefficients.put(Integer.parseInt(entry.getKey()),
                        Double.parseDouble(entry.getValue()));
            }
        }
        return new ColumnModelArtifact(row.getAs("model_name"),
                row.getAs("model_version"), row.getAs("column_name"),
                ClassifierType.valueOf(row.getAs("classifier_type")),
                row.getAs("dictionary_version"), row.getAs("feature_dimension"),
                row.getAs("threshold"), row.getAs("intercept"), coefficients,
                row.getAs("training_mode"));
    }

    private FeatureDictionary findDictionary(String version) {
        if (!tableGateway.tableExists(dictionaryTable)) {
            return null;
        }
        List<Row> rows = tableGateway.read(dictionaryTable)
                .filter(functions.col("dictionary_version").equalTo(version))
                .orderBy("feature_index").collectAsList();
        if (rows.isEmpty()) {
            return null;
        }
        String columnName = rows.get(0).getAs("column_name");
        long createdAt = rows.get(0).getAs("created_at");
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        for (Row row : rows) {
            int index = row.getAs("feature_index");
            if (!columnName.equals(row.getAs("column_name"))
                    || createdAt != ((Long) row.getAs("created_at"))) {
                throw new IllegalStateException("FMDB 特征字典元数据不一致：" + version);
            }
            if (index == EMPTY_DICTIONARY_INDEX) {
                continue;
            }
            definitions.put(index, new FeatureDefinition(index,
                    row.getAs("feature_name"),
                    FeatureType.valueOf(row.getAs("feature_type")),
                    row.getAs("feature_source"), row.getAs("default_value")));
        }
        return new FeatureDictionary(version, columnName, definitions, createdAt);
    }

    private String modelPath(String version) {
        return MODEL_SCHEME + modelTable + "/" + version;
    }

    private String dictionaryPath(String version) {
        return MODEL_SCHEME + dictionaryTable + "/" + version;
    }

    private String versionFromPath(String path) {
        String validated = ValueUtils.requireNotBlank(path, "FMDB 模型路径");
        String prefix = MODEL_SCHEME + modelTable + "/";
        if (!validated.startsWith(prefix) || validated.length() == prefix.length()) {
            throw new IllegalArgumentException("FMDB 模型路径不属于当前模型表");
        }
        return validated.substring(prefix.length());
    }

    private long positiveNow() {
        return Math.max(1L, clock.millis());
    }

    private static boolean sameModel(ColumnModelArtifact first,
                                     ColumnModelArtifact second) {
        return first.getModelName().equals(second.getModelName())
                && first.getModelVersion().equals(second.getModelVersion())
                && first.getColumnName().equals(second.getColumnName())
                && first.getClassifierType() == second.getClassifierType()
                && first.getFeatureDictionaryVersion().equals(
                second.getFeatureDictionaryVersion())
                && first.getFeatureDimension() == second.getFeatureDimension()
                && Double.compare(first.getThreshold(), second.getThreshold()) == 0
                && Double.compare(first.getIntercept(), second.getIntercept()) == 0
                && first.getCoefficients().equals(second.getCoefficients())
                && first.getTrainingMode().equals(second.getTrainingMode());
    }

    private static boolean sameDictionary(FeatureDictionary first,
                                          FeatureDictionary second) {
        if (!first.getVersion().equals(second.getVersion())
                || !first.getColumnName().equals(second.getColumnName())
                || first.getCreatedAt() != second.getCreatedAt()
                || first.getDefinitions().size() != second.getDefinitions().size()) {
            return false;
        }
        for (Map.Entry<Integer, FeatureDefinition> entry
                : first.getDefinitions().entrySet()) {
            FeatureDefinition other = second.getDefinitions().get(entry.getKey());
            FeatureDefinition value = entry.getValue();
            if (other == null || value.getIndex() != other.getIndex()
                    || !value.getName().equals(other.getName())
                    || value.getFeatureType() != other.getFeatureType()
                    || !value.getSource().equals(other.getSource())
                    || Double.compare(value.getDefaultValue(), other.getDefaultValue()) != 0) {
                return false;
            }
        }
        return true;
    }

    private static StructField field(String name, DataType type, boolean nullable) {
        return DataTypes.createStructField(name, type, nullable);
    }
}
