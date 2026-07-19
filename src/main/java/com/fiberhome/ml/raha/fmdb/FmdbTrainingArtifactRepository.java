package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataset;
import com.fiberhome.ml.raha.model.training.ColumnTrainingExample;
import com.fiberhome.ml.raha.model.training.ColumnTrainingStatus;
import com.fiberhome.ml.raha.data.type.LabelSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 批量追加训练列级产物、训练单元格和最终训练样本，并提供分区读取边界。
 */
public final class FmdbTrainingArtifactRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbTrainingArtifactRepository.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 列级产物表名。 */
    private final String columnArtifactTable;
    /** 训练单元格表名。 */
    private final String trainingCellTable;
    /** 训练样本表名。 */
    private final String trainingExampleTable;

    public FmdbTrainingArtifactRepository(SparkSession sparkSession,
                                           FmdbTableGateway tableGateway,
                                           FmdbPersistenceConfig persistenceConfig) {
        if (sparkSession == null || tableGateway == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 训练产物仓储依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.columnArtifactTable = FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT
                .getTableName();
        this.trainingCellTable = FmdbPhysicalTable.TRAINING_CELL.getTableName();
        this.trainingExampleTable = FmdbPhysicalTable.TRAINING_EXAMPLE.getTableName();
    }

    public long saveColumnArtifacts(
            List<FmdbTrainingColumnArtifactRecord> records) {
        if (records == null) {
            throw new IllegalArgumentException("训练列产物记录不能为空");
        }
        if (!persistenceConfig.shouldPersist(
                FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT)) {
            LOGGER.info("训练列级产物入库已关闭，跳过写入，recordCount={}", records.size());
            return 0L;
        }
        List<Row> rows = new ArrayList<Row>(records.size());
        for (FmdbTrainingColumnArtifactRecord record : records) {
            if (record == null) {
                throw new IllegalArgumentException("训练列产物不能包含空记录");
            }
            rows.add(FmdbTableRecord.of(FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT,
                    artifactValues(record)).toRow());
        }
        return append(FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT,
                columnArtifactTable, rows,
                Arrays.asList("training_batch_id", "column_name"));
    }

    public boolean isColumnArtifactPersistenceEnabled() {
        return persistenceConfig.shouldPersist(
                FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT);
    }

    public boolean isTrainingCellPersistenceEnabled() {
        return persistenceConfig.shouldPersist(FmdbPhysicalTable.TRAINING_CELL);
    }

    public boolean isTrainingExamplePersistenceEnabled() {
        return persistenceConfig.shouldPersist(FmdbPhysicalTable.TRAINING_EXAMPLE);
    }

    public long saveTrainingCells(List<FmdbTrainingCellRecord> records) {
        return saveTrainingCells(records, persistenceConfig.shouldPersist(
                FmdbPhysicalTable.TRAINING_CELL));
    }

    public long saveTrainingCells(List<FmdbTrainingCellRecord> records,
                                  boolean enabled) {
        if (records == null) {
            throw new IllegalArgumentException("训练单元格记录不能为空");
        }
        if (!enabled || !persistenceConfig.shouldPersist(FmdbPhysicalTable.TRAINING_CELL)) {
            LOGGER.info("训练单元格入库已关闭，跳过写入，recordCount={}", records.size());
            return 0L;
        }
        List<Row> rows = new ArrayList<Row>(records.size());
        for (FmdbTrainingCellRecord record : records) {
            if (record == null) {
                throw new IllegalArgumentException("训练单元格不能包含空记录");
            }
            rows.add(FmdbTableRecord.of(FmdbPhysicalTable.TRAINING_CELL,
                    record.values()).toRow());
        }
        return append(FmdbPhysicalTable.TRAINING_CELL, trainingCellTable, rows,
                Arrays.asList("training_batch_id", "dataset_id", "cell_id"));
    }

    public long saveTrainingExamples(List<FmdbTrainingExampleRecord> records) {
        if (records == null) {
            throw new IllegalArgumentException("训练样本记录不能为空");
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.TRAINING_EXAMPLE)) {
            LOGGER.info("最终训练样本入库已关闭，跳过写入，recordCount={}", records.size());
            return 0L;
        }
        List<Row> rows = new ArrayList<Row>(records.size());
        for (FmdbTrainingExampleRecord record : records) {
            if (record == null) {
                throw new IllegalArgumentException("训练样本不能包含空记录");
            }
            rows.add(FmdbTableRecord.of(FmdbPhysicalTable.TRAINING_EXAMPLE,
                    record.values()).toRow());
        }
        return append(FmdbPhysicalTable.TRAINING_EXAMPLE, trainingExampleTable,
                rows, Arrays.asList("model_set_version", "row_id", "column_name"));
    }

    public Dataset<Row> findTrainingCells(String datasetId,
                                          String trainingBatchId,
                                          String columnName) {
        return tableGateway.read(trainingCellTable,
                FmdbTableSchemas.columns(FmdbPhysicalTable.TRAINING_CELL),
                org.apache.spark.sql.functions.col("dataset_id").equalTo(datasetId)
                        .and(org.apache.spark.sql.functions.col("training_batch_id")
                                .equalTo(trainingBatchId))
                        .and(org.apache.spark.sql.functions.col("column_name")
                                .equalTo(columnName)));
    }

    public Dataset<Row> findTrainingExamples(String datasetId,
                                              String partitionMonth,
                                              String modelSetVersion,
                                              String columnName) {
        return tableGateway.read(trainingExampleTable,
                FmdbTableSchemas.columns(FmdbPhysicalTable.TRAINING_EXAMPLE),
                org.apache.spark.sql.functions.col("dataset_id").equalTo(datasetId)
                        .and(org.apache.spark.sql.functions.col("partition_month")
                                .equalTo(partitionMonth))
                        .and(org.apache.spark.sql.functions.col("model_set_version")
                                .equalTo(modelSetVersion))
                        .and(org.apache.spark.sql.functions.col("column_name")
                                .equalTo(columnName)));
    }

    /**
     * 从冻结训练样本恢复单列训练数据，恢复结果不依赖当前进程内存。
     */
    public ColumnTrainingDataset loadFrozenTrainingDataset(
            String datasetId,
            String partitionMonth,
            String modelSetVersion,
            String columnName,
            String featureDictionaryVersion,
            int featureDimension) {
        if (!isTrainingExamplePersistenceEnabled()) {
            throw new IllegalStateException("最终训练样本持久化已关闭，不能恢复训练");
        }
        if (!tableGateway.tableExists(trainingExampleTable)) {
            return new ColumnTrainingDataset(columnName, featureDictionaryVersion,
                    featureDimension, Collections.<ColumnTrainingExample>emptyList(),
                    0, 0, 0, ColumnTrainingStatus.NO_LABELS, "冻结样本为空");
        }
        Dataset<Row> frame = findTrainingExamples(datasetId, partitionMonth,
                modelSetVersion, columnName);
        List<ColumnTrainingExample> examples = new ArrayList<ColumnTrainingExample>();
        int positives = 0;
        for (Row row : frame.collectAsList()) {
            String actualVersion = (String) row.getAs("feature_dictionary_version");
            if (!featureDictionaryVersion.equals(actualVersion)) {
                throw new IllegalStateException("冻结训练样本字典版本不一致：" + columnName);
            }
            Map<Integer, Double> values = featureValues(
                    (String) row.getAs("feature_vector_json"));
            int label = ((Number) row.getAs("label")).intValue();
            LabelSource source = LabelSource.valueOf(
                    (String) row.getAs("label_source"));
            double weight = ((Number) row.getAs("sample_weight")).doubleValue();
            examples.add(new ColumnTrainingExample(
                    (String) row.getAs("cell_id"), label, source, values, weight));
            positives += label;
        }
        int negatives = examples.size() - positives;
        ColumnTrainingStatus status = examples.isEmpty()
                ? ColumnTrainingStatus.NO_LABELS
                : positives == 0 || negatives == 0
                ? ColumnTrainingStatus.SINGLE_CLASS : ColumnTrainingStatus.TRAINABLE;
        return new ColumnTrainingDataset(columnName, featureDictionaryVersion,
                featureDimension, examples, positives, negatives, 0, status,
                examples.isEmpty() ? "冻结样本为空" : "冻结样本已恢复");
    }

    /** 返回指定训练批次的列级产物条数，用于幂等重试后的物理校验。 */
    public long countColumnArtifacts(String datasetId, String trainingBatchId) {
        if (!tableGateway.tableExists(columnArtifactTable)) {
            return 0L;
        }
        return tableGateway.read(columnArtifactTable,
                Arrays.asList("training_batch_id", "dataset_id"),
                org.apache.spark.sql.functions.col("training_batch_id")
                        .equalTo(trainingBatchId)
                        .and(org.apache.spark.sql.functions.col("dataset_id")
                                .equalTo(datasetId))).count();
    }

    /** 返回指定训练批次的训练单元格条数。 */
    public long countTrainingCells(String datasetId, String trainingBatchId) {
        if (!tableGateway.tableExists(trainingCellTable)) {
            return 0L;
        }
        return tableGateway.read(trainingCellTable,
                Arrays.asList("training_batch_id", "dataset_id"),
                org.apache.spark.sql.functions.col("training_batch_id")
                        .equalTo(trainingBatchId)
                        .and(org.apache.spark.sql.functions.col("dataset_id")
                                .equalTo(datasetId))).count();
    }

    /** 返回指定模型集合的最终训练样本条数。 */
    public long countTrainingExamples(String datasetId, String partitionMonth,
                                      String modelSetVersion) {
        if (!tableGateway.tableExists(trainingExampleTable)) {
            return 0L;
        }
        return tableGateway.read(trainingExampleTable,
                Arrays.asList("dataset_id", "partition_month", "model_set_version"),
                org.apache.spark.sql.functions.col("dataset_id").equalTo(datasetId)
                        .and(org.apache.spark.sql.functions.col("partition_month")
                                .equalTo(partitionMonth))
                        .and(org.apache.spark.sql.functions.col("model_set_version")
                                .equalTo(modelSetVersion))).count();
    }

    private static Map<Integer, Double> featureValues(String json) {
        Map<String, Object> raw = FmdbJsonCodec.readObject(json);
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            try {
                values.put(Integer.valueOf(entry.getKey()),
                        ((Number) entry.getValue()).doubleValue());
            } catch (RuntimeException exception) {
                throw new IllegalStateException("冻结训练样本特征向量非法", exception);
            }
        }
        return values;
    }

    private long append(FmdbPhysicalTable table,
                        String tableName,
                        List<Row> rows,
                        List<String> keys) {
        if (rows.isEmpty()) {
            return 0L;
        }
        Dataset<Row> frame = sparkSession.createDataFrame(rows,
                FmdbTableSchemas.schema(table));
        LOGGER.info("开始追加训练物理产物，tableName={}，recordCount={}",
                tableName, rows.size());
        try {
            long written = tableGateway.appendIdempotent(tableName, frame, keys);
            LOGGER.info("训练物理产物追加完成，tableName={}，writtenCount={}，"
                            + "skippedCount={}", tableName, written,
                    rows.size() - written);
            return written;
        } catch (RuntimeException exception) {
            LOGGER.error("训练物理产物追加失败，tableName={}，recordCount={}",
                    tableName, rows.size(), exception);
            throw exception;
        }
    }

    private Map<String, Object> artifactValues(
            FmdbTrainingColumnArtifactRecord record) {
        Map<String, Object> values = record.values();
        clearDisabled(values, FmdbColumnArtifact.PROFILE,
                "profile_version", "profile_json");
        clearDisabled(values, FmdbColumnArtifact.STRATEGY_PLAN,
                "strategy_plan_version", "strategy_plan_json");
        clearDisabled(values, FmdbColumnArtifact.FEATURE_DICTIONARY,
                "feature_dictionary_version", "feature_dictionary_json");
        clearDisabled(values, FmdbColumnArtifact.CLUSTER_SUMMARY,
                "cluster_version", "cluster_summary_json");
        clearDisabled(values, FmdbColumnArtifact.PROPAGATION_SUMMARY,
                "propagation_summary_json");
        return values;
    }

    private void clearDisabled(Map<String, Object> values,
                               FmdbColumnArtifact artifact,
                               String... fields) {
        if (!persistenceConfig.shouldPersist(artifact)) {
            for (String field : fields) {
                values.put(field, null);
            }
        }
    }
}
