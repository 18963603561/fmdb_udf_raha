package com.fiberhome.ml.raha.repository.adapter.fmdb;

import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.model.ColumnModelStore;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.ModelPersistenceContext;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.SparkSqlFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbColumnArtifact;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbFeatureDictionaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用最终模型产物表保存列模型，并从训练列级产物表加载特征字典。
 */
public final class FmdbModelStore implements ColumnModelStore {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FmdbModelStore.class);
    /** FMDB 模型路径协议。 */
    private static final String MODEL_SCHEME = "fmdb://";
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 最终模型产物表。 */
    private final String modelTable;
    /** 训练列级产物表。 */
    private final String columnArtifactTable;
    /** 提供可测试存储时间的时钟。 */
    private final Clock clock;
    /** 统一持久化配置。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 按完整路径缓存模型。 */
    private final Map<String, ColumnModelArtifact> modelCache =
            new ConcurrentHashMap<String, ColumnModelArtifact>();
    /** 按字典版本缓存字典。 */
    private final Map<String, FeatureDictionary> dictionaryCache =
            new ConcurrentHashMap<String, FeatureDictionary>();

    public FmdbModelStore(SparkSession sparkSession,
                          FmdbTableGateway tableGateway,
                          String modelTable,
                          String columnArtifactTable,
                          Clock clock,
                          FmdbPersistenceConfig persistenceConfig) {
        if (sparkSession == null || tableGateway == null || clock == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 模型存储依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.modelTable = SparkSqlFmdbTableGateway.validateTableName(modelTable);
        this.columnArtifactTable = SparkSqlFmdbTableGateway.validateTableName(
                columnArtifactTable);
        this.clock = clock;
        this.persistenceConfig = persistenceConfig;
    }

    @Override
    public synchronized String save(ColumnModelArtifact artifact,
                                    ModelPersistenceContext context) {
        if (artifact == null || context == null) {
            throw new IllegalArgumentException("FMDB 模型参数和上下文不能为空");
        }
        String path = modelPath(artifact.getModelVersion());
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.MODEL_ARTIFACT)) {
            modelCache.put(path, artifact);
            LOGGER.info("FMDB 模型产物入库已关闭，仅缓存当前模型，modelVersion={}，"
                            + "columnName={}，configKey={}", artifact.getModelVersion(),
                    artifact.getColumnName(),
                    FmdbPhysicalTable.MODEL_ARTIFACT.getConfigKey());
            return path;
        }
        ColumnModelArtifact existing = findModel(artifact.getModelVersion());
        if (existing != null && !sameModel(existing, artifact)) {
            throw new IllegalStateException("FMDB 中同一模型版本已存在不同参数");
        }
        long storedAt = Math.max(context.getCreatedAt(), positiveNow());
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("coefficients", artifact.getCoefficients());
        payload.put("intercept", artifact.getIntercept());
        payload.put("modelName", artifact.getModelName());
        payload.put("trainingMode", artifact.getTrainingMode());
        payload.put("rowIdentityMode",
                context.getRowIdentityConfig().getMode().name());
        payload.put("rowKeyColumns",
                context.getRowIdentityConfig().getKeyColumns());
        payload.put("rowFingerprintAlgorithm",
                context.getRowIdentityConfig().getFingerprintAlgorithm().name());
        payload.put("rowFingerprintVersion",
                context.getRowIdentityConfig().getNormalizationVersion());
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("model_set_version", context.getModelSetVersion());
        values.put("dataset_id", context.getDatasetId());
        values.put("schema_hash", context.getSchemaHash());
        values.put("training_batch_id", context.getTrainingBatchId());
        values.put("model_set_status", context.getStatus().name());
        values.put("state_version", 1);
        values.put("strategy_plan_version", context.getStrategyPlanVersion());
        values.put("merge_algorithm_version", context.getMergeAlgorithmVersion());
        values.put("column_name", artifact.getColumnName());
        values.put("model_version", artifact.getModelVersion());
        values.put("classifier_type", artifact.getClassifierType().name());
        values.put("feature_dictionary_version",
                artifact.getFeatureDictionaryVersion());
        values.put("feature_dimension", artifact.getFeatureDimension());
        values.put("threshold", artifact.getThreshold());
        values.put("model_path", path);
        values.put("model_payload_json", FmdbJsonCodec.write(payload));
        values.put("metrics_json", FmdbJsonCodec.write(context.getMetrics()));
        values.put("created_at", storedAt);
        values.put("published_at", context.getPublishedAt());
        Dataset<Row> frame = sparkSession.createDataFrame(
                Collections.singletonList(FmdbTableRecord.of(
                        FmdbPhysicalTable.MODEL_ARTIFACT, values).toRow()),
                FmdbTableSchemas.schema(FmdbPhysicalTable.MODEL_ARTIFACT));
        tableGateway.append(modelTable, frame,
                Arrays.asList("model_set_version", "column_name",
                        "model_version", "state_version"), 1L);
        modelCache.put(path, artifact);
        LOGGER.info("FMDB 列级模型保存完成，modelSetVersion={}，modelVersion={}，"
                        + "columnName={}", context.getModelSetVersion(),
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
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.MODEL_ARTIFACT)) {
            throw new IllegalStateException("FMDB 模型产物入库已关闭且当前缓存不存在模型："
                    + version);
        }
        ColumnModelArtifact artifact = findModel(version);
        if (artifact == null) {
            throw new IllegalStateException("FMDB 中不存在模型版本：" + version);
        }
        modelCache.put(modelPath, artifact);
        return artifact;
    }

    /**
     * 从训练列级产物表按不可变版本加载特征字典。
     */
    public FeatureDictionary loadDictionary(String dictionaryVersion) {
        String version = ValueUtils.requireNotBlank(
                dictionaryVersion, "FMDB 特征字典版本");
        FeatureDictionary cached = dictionaryCache.get(version);
        if (cached != null) {
            return cached;
        }
        if (!persistenceConfig.shouldPersist(FmdbColumnArtifact.FEATURE_DICTIONARY)) {
            throw new IllegalStateException("FMDB 特征字典入库已关闭且当前缓存不存在字典："
                    + version);
        }
        if (!tableGateway.tableExists(columnArtifactTable)) {
            throw new IllegalStateException("FMDB 训练列级产物表不存在："
                    + columnArtifactTable);
        }
        List<Row> rows = tableGateway.read(columnArtifactTable,
                        Arrays.asList("feature_dictionary_version",
                                "feature_dictionary_json", "created_at"),
                        functions.col("feature_dictionary_version").equalTo(version))
                .orderBy(functions.col("created_at").desc()).limit(2).collectAsList();
        if (rows.isEmpty()) {
            throw new IllegalStateException("FMDB 中不存在特征字典版本：" + version);
        }
        FeatureDictionary dictionary = FmdbFeatureDictionaryCodec.read(
                (String) rows.get(0).getAs("feature_dictionary_json"));
        if (!version.equals(dictionary.getVersion())) {
            throw new IllegalStateException("FMDB 特征字典版本与 JSON 内容不一致："
                    + version);
        }
        dictionaryCache.put(version, dictionary);
        return dictionary;
    }

    private ColumnModelArtifact findModel(String version) {
        if (!tableGateway.tableExists(modelTable)) {
            return null;
        }
        List<Row> rows = tableGateway.read(modelTable,
                        FmdbTableSchemas.columns(FmdbPhysicalTable.MODEL_ARTIFACT),
                        functions.col("model_version").equalTo(version))
                .orderBy(functions.when(functions.col("model_set_status")
                                .equalTo("PUBLISHED"), 0).otherwise(1),
                        functions.col("state_version").desc(),
                        functions.col("published_at").desc_nulls_last(),
                        functions.col("created_at").desc())
                .limit(1).collectAsList();
        if (rows.isEmpty()) {
            return null;
        }
        Row row = rows.get(0);
        Map<String, Object> payload = FmdbJsonCodec.readObject(
                row.getAs("model_payload_json"));
        Map<Integer, Double> coefficients = coefficients(payload.get("coefficients"));
        return new ColumnModelArtifact(text(payload, "modelName"),
                row.getAs("model_version"), row.getAs("column_name"),
                ClassifierType.valueOf(row.getAs("classifier_type")),
                row.getAs("feature_dictionary_version"),
                row.getAs("feature_dimension"), row.getAs("threshold"),
                number(payload, "intercept").doubleValue(), coefficients,
                text(payload, "trainingMode"));
    }

    @SuppressWarnings("unchecked")
    private static Map<Integer, Double> coefficients(Object raw) {
        if (!(raw instanceof Map)) {
            throw new IllegalArgumentException("模型 JSON 缺少 coefficients");
        }
        Map<Integer, Double> result = new LinkedHashMap<Integer, Double>();
        for (Map.Entry<Object, Object> entry
                : ((Map<Object, Object>) raw).entrySet()) {
            int index = Integer.parseInt(String.valueOf(entry.getKey()));
            if (!(entry.getValue() instanceof Number)) {
                throw new IllegalArgumentException("模型系数必须为数值：" + index);
            }
            result.put(index, ((Number) entry.getValue()).doubleValue());
        }
        return result;
    }

    private static String text(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
            throw new IllegalArgumentException("模型 JSON 缺少字段：" + key);
        }
        return (String) value;
    }

    private static Number number(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("模型 JSON 缺少数值字段：" + key);
        }
        return (Number) value;
    }

    private String modelPath(String version) {
        String validated = ValueUtils.requireNotBlank(version, "FMDB 模型版本");
        if (validated.contains("/") || validated.contains("\\")
                || validated.contains(":")) {
            throw new IllegalArgumentException("FMDB 模型版本不能包含路径分隔符");
        }
        return MODEL_SCHEME + modelTable + "/" + validated;
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
}
