package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbFeatureDictionaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.FeatureRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 暂存当前任务特征，并从列级产物和训练单元格表恢复字典及向量。
 */
public final class FmdbFeatureRepository implements FeatureRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbFeatureRepository.class);
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 当前任务尚未统一物化的特征字典。 */
    private final Map<String, FeatureDictionary> pendingDictionaries =
            new LinkedHashMap<String, FeatureDictionary>();
    /** 当前任务尚未统一物化的特征向量。 */
    private final Map<String, List<SparseFeatureRow>> pendingRows =
            new LinkedHashMap<String, List<SparseFeatureRow>>();
    /** 训练列级产物表名。 */
    private final String columnArtifactTable;
    /** 训练单元格表名。 */
    private final String trainingCellTable;

    public FmdbFeatureRepository(FmdbTableGateway tableGateway,
                                 FmdbPersistenceConfig persistenceConfig) {
        if (tableGateway == null || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 特征仓储依赖不能为空");
        }
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.columnArtifactTable = FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT
                .getTableName();
        this.trainingCellTable = FmdbPhysicalTable.TRAINING_CELL.getTableName();
    }

    @Override
    public synchronized void save(String jobId,
                                  FeatureAssemblyResult result,
                                  ArtifactVersion version,
                                  long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (result == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("特征结果、版本和更新时间必须有效");
        }
        for (Map.Entry<String, FeatureDictionary> entry
                : result.getDictionaries().entrySet()) {
            pendingDictionaries.put(key(validatedJobId, entry.getKey()),
                    entry.getValue());
        }
        Map<String, List<SparseFeatureRow>> grouped =
                new LinkedHashMap<String, List<SparseFeatureRow>>();
        for (SparseFeatureRow row : result.getRows()) {
            if (!grouped.containsKey(row.getColumnName())) {
                grouped.put(row.getColumnName(), new ArrayList<SparseFeatureRow>());
            }
            grouped.get(row.getColumnName()).add(row);
        }
        for (Map.Entry<String, List<SparseFeatureRow>> entry : grouped.entrySet()) {
            pendingRows.put(key(validatedJobId, entry.getKey()),
                    Collections.unmodifiableList(
                            new ArrayList<SparseFeatureRow>(entry.getValue())));
        }
        LOGGER.info("特征阶段保存完成，jobId={}，dictionaryCount={}，rowCount={}",
                validatedJobId, result.getDictionaries().size(), result.getRows().size());
    }

    @Override
    public synchronized Optional<FeatureDictionary> findDictionary(
            String jobId, String columnName) {
        String key = key(jobId, columnName);
        FeatureDictionary pending = pendingDictionaries.get(key);
        if (pending != null) {
            return Optional.of(pending);
        }
        if (!persistenceConfig.shouldPersist(
                FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT)
                || !tableGateway.tableExists(columnArtifactTable)) {
            return Optional.empty();
        }
        List<Row> rows = tableGateway.read(columnArtifactTable,
                java.util.Arrays.asList("feature_dictionary_json", "created_at"),
                functions.col("training_batch_id").equalTo(jobId)
                        .and(functions.col("column_name").equalTo(columnName))
                        .and(functions.col("feature_dictionary_json").isNotNull()))
                .orderBy(functions.col("created_at").desc()).limit(1).collectAsList();
        LOGGER.debug("FMDB 特征字典恢复完成，jobId={}，columnName={}，found={}",
                jobId, columnName, !rows.isEmpty());
        return rows.isEmpty() ? Optional.empty()
                : Optional.of(FmdbFeatureDictionaryCodec.read(
                (String) rows.get(0).getAs("feature_dictionary_json")));
    }

    @Override
    public synchronized List<SparseFeatureRow> findRows(String jobId,
                                                        String columnName) {
        String key = key(jobId, columnName);
        List<SparseFeatureRow> pending = pendingRows.get(key);
        if (pending != null) {
            return pending;
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.TRAINING_CELL)
                || !tableGateway.tableExists(trainingCellTable)) {
            return Collections.emptyList();
        }
        List<Row> rows = tableGateway.read(trainingCellTable,
                java.util.Arrays.asList("dataset_id", "training_snapshot_id",
                        "row_id", "column_name", "cell_id", "cell_value",
                        "feature_dictionary_version", "feature_vector_json",
                        "feature_summary_json"),
                functions.col("training_batch_id").equalTo(jobId)
                        .and(functions.col("column_name").equalTo(columnName)))
                .collectAsList();
        List<SparseFeatureRow> result = new ArrayList<SparseFeatureRow>(rows.size());
        for (Row row : rows) {
            String value = row.getAs("cell_value");
            CellCoordinate coordinate = new CellCoordinate(
                    (String) row.getAs("dataset_id"),
                    (String) row.getAs("training_snapshot_id"),
                    (String) row.getAs("row_id"),
                    (String) row.getAs("column_name"));
            String valueHash = HashUtils.md5Hex(value == null ? "<null>" : value);
            result.add(new SparseFeatureRow((String) row.getAs("cell_id"),
                    coordinate.getColumnName(), coordinate, valueHash, null,
                    (String) row.getAs("feature_dictionary_version"),
                    featureValues((String) row.getAs("feature_vector_json")),
                    stringValues((String) row.getAs("feature_summary_json"))));
        }
        Collections.sort(result, Comparator.comparing(SparseFeatureRow::getCellId));
        LOGGER.debug("FMDB 特征向量恢复完成，jobId={}，columnName={}，rowCount={}",
                jobId, columnName, result.size());
        return Collections.unmodifiableList(result);
    }

    private static Map<Integer, Double> featureValues(String json) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        for (Map.Entry<String, Object> entry : FmdbJsonCodec.readObject(json).entrySet()) {
            if (!(entry.getValue() instanceof Number)) {
                throw new IllegalArgumentException("特征向量 JSON 包含非数值字段");
            }
            values.put(Integer.valueOf(entry.getKey()),
                    ((Number) entry.getValue()).doubleValue());
        }
        return values;
    }

    private static Map<String, String> stringValues(String json) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Object> entry : FmdbJsonCodec.readObject(json).entrySet()) {
            if (!(entry.getValue() instanceof String)) {
                throw new IllegalArgumentException("特征摘要 JSON 包含非文本字段");
            }
            values.put(entry.getKey(), (String) entry.getValue());
        }
        return values;
    }

    private static String key(String jobId, String columnName) {
        String job = ValueUtils.requireNotBlank(jobId, "任务标识");
        String column = ValueUtils.requireNotBlank(columnName, "字段名称");
        return job.length() + ":" + job + column.length() + ":" + column;
    }
}
