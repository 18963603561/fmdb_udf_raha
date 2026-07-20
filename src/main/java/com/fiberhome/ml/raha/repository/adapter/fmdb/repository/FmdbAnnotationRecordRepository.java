package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRecord;
import com.fiberhome.ml.raha.annotation.domain.RowAnnotation;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbRawValueAccessPolicy;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用最终标注宽表追加保存不可变标注批次，并按分区读取批次或修订版本。
 */
public final class FmdbAnnotationRecordRepository
        implements AnnotationRecordRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbAnnotationRecordRepository.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化总开关和标注表开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 最终标注物理表名。 */
    private final String tableName;
    /** 标注原始行访问策略。 */
    private final FmdbRawValueAccessPolicy rawValueAccessPolicy;

    public FmdbAnnotationRecordRepository(
            SparkSession sparkSession,
            FmdbTableGateway tableGateway,
            FmdbPersistenceConfig persistenceConfig) {
        this(sparkSession, tableGateway, persistenceConfig,
                FmdbPhysicalTable.ANNOTATION_RECORD.getTableName(),
                FmdbRawValueAccessPolicy.allowAllInternal());
    }

    public FmdbAnnotationRecordRepository(
            SparkSession sparkSession,
            FmdbTableGateway tableGateway,
            FmdbPersistenceConfig persistenceConfig,
            String tableName) {
        this(sparkSession, tableGateway, persistenceConfig, tableName,
                FmdbRawValueAccessPolicy.allowAllInternal());
    }

    public FmdbAnnotationRecordRepository(
            SparkSession sparkSession,
            FmdbTableGateway tableGateway,
            FmdbPersistenceConfig persistenceConfig,
            String tableName,
            FmdbRawValueAccessPolicy rawValueAccessPolicy) {
        if (sparkSession == null || tableGateway == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 标注仓储依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = ValueUtils.requireNotBlank(tableName, "FMDB 标注表名");
        if (rawValueAccessPolicy == null) {
            throw new IllegalArgumentException("标注原始行访问策略不能为空");
        }
        this.rawValueAccessPolicy = rawValueAccessPolicy;
    }

    @Override
    public boolean isPersistenceEnabled() {
        return persistenceConfig.shouldPersist(
                FmdbPhysicalTable.ANNOTATION_RECORD);
    }

    @Override
    public long saveAll(AnnotationBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("FMDB 标注批次不能为空");
        }
        if (!isPersistenceEnabled()) {
            LOGGER.info("FMDB 标注记录入库已关闭，跳过写入，annotationBatchId={}，"
                            + "recordCount={}，configKey={}",
                    batch.getAnnotationBatchId(), batch.getRecords().size(),
                    FmdbPhysicalTable.ANNOTATION_RECORD.getConfigKey());
            return 0L;
        }
        List<Row> rows = new ArrayList<Row>(batch.getRecords().size());
        for (AnnotationRecord record : batch.getRecords()) {
            rows.add(toTableRecord(record).toRow());
        }
        Dataset<Row> frame = sparkSession.createDataFrame(rows,
                FmdbTableSchemas.schema(FmdbPhysicalTable.ANNOTATION_RECORD));
        LOGGER.info("开始追加 FMDB 标注批次，annotationBatchId={}，recordCount={}",
                batch.getAnnotationBatchId(), rows.size());
        try {
            long written = tableGateway.appendIdempotent(tableName, frame,
                    Arrays.asList("annotation_batch_id", "row_id"));
            LOGGER.info("FMDB 标注批次追加完成，annotationBatchId={}，"
                            + "writtenCount={}，skippedCount={}",
                    batch.getAnnotationBatchId(), written, rows.size() - written);
            return written;
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 标注批次追加失败，annotationBatchId={}，"
                            + "recordCount={}", batch.getAnnotationBatchId(),
                    rows.size(), exception);
            throw exception;
        }
    }

    @Override
    public Optional<AnnotationBatch> find(String datasetId,
                                          String partitionMonth,
                                          String annotationBatchId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "标注查询数据集");
        String month = ValueUtils.requireNotBlank(partitionMonth, "标注查询月分区");
        String batchId = ValueUtils.requireNotBlank(
                annotationBatchId, "标注查询批次");
        rawValueAccessPolicy.requireRead(dataset, "读取标注批次原始行");
        if (!tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        LOGGER.debug("开始分区裁剪读取标注批次，datasetId={}，"
                        + "partitionMonth={}，annotationBatchId={}",
                dataset, month, batchId);
        List<Row> rows = readRows(functions.col("dataset_id").equalTo(dataset)
                .and(functions.col("partition_month").equalTo(month))
                .and(functions.col("annotation_batch_id").equalTo(batchId)));
        return rows.isEmpty() ? Optional.<AnnotationBatch>empty()
                : Optional.of(fromRows(rows));
    }

    @Override
    public Optional<AnnotationBatch> findLatestForSample(
            String datasetId,
            String partitionMonth,
            String sampleBatchId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "标注查询数据集");
        String month = ValueUtils.requireNotBlank(partitionMonth, "标注查询月分区");
        String sample = ValueUtils.requireNotBlank(sampleBatchId, "采样批次标识");
        rawValueAccessPolicy.requireRead(dataset, "读取最新标注原始行");
        if (!tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        Dataset<Row> selected = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.ANNOTATION_RECORD),
                functions.col("dataset_id").equalTo(dataset)
                        .and(functions.col("partition_month").equalTo(month))
                        .and(functions.col("sample_batch_id").equalTo(sample)));
        List<Row> rows = selected.orderBy(
                functions.col("annotated_at").desc(),
                functions.col("annotation_batch_id").desc(),
                functions.col("row_id").asc()).collectAsList();
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        String latestBatch = rows.get(0).getAs("annotation_batch_id");
        List<Row> latest = new ArrayList<Row>();
        for (Row row : rows) {
            if (latestBatch.equals(row.getAs("annotation_batch_id"))) {
                latest.add(row);
            }
        }
        return Optional.of(fromRows(latest));
    }

    @Override
    public Optional<AnnotationBatch> findLatestTrainableForSample(
            String sampleBatchId,
            boolean allowPartial) {
        String sample = ValueUtils.requireNotBlank(
                sampleBatchId, "全局采样批次标识");
        if (!tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        org.apache.spark.sql.Column status = functions.col("batch_status")
                .equalTo(AnnotationBatchStatus.IMPORTED.name());
        if (allowPartial) {
            status = status.or(functions.col("batch_status")
                    .equalTo(AnnotationBatchStatus.PARTIAL.name()));
        }
        LOGGER.debug("开始定位训练可用标注批次，sampleBatchId={}，allowPartial={}",
                sample, allowPartial);
        List<Row> locations = tableGateway.read(tableName,
                Arrays.asList("dataset_id", "partition_month",
                        "annotation_batch_id", "annotated_at"),
                functions.col("sample_batch_id").equalTo(sample).and(status))
                .orderBy(functions.col("annotated_at").desc(),
                        functions.col("annotation_batch_id").desc())
                .collectAsList();
        if (locations.isEmpty()) {
            return Optional.empty();
        }
        Row latest = locations.get(0);
        String datasetId = latest.getAs("dataset_id");
        String month = latest.getAs("partition_month");
        String annotationBatchId = latest.getAs("annotation_batch_id");
        for (Row row : locations) {
            if (!datasetId.equals(row.getAs("dataset_id"))) {
                throw new IllegalStateException("采样批次跨多个数据集存在标注：" + sample);
            }
        }
        rawValueAccessPolicy.requireRead(datasetId, "读取训练标注批次原始行");
        LOGGER.debug("训练标注批次定位完成，sampleBatchId={}，"
                        + "annotationBatchId={}，partitionMonth={}", sample,
                annotationBatchId, month);
        return find(datasetId, month, annotationBatchId);
    }

    @Override
    public boolean existsImportFingerprint(String datasetId,
                                           String sampleBatchId,
                                           String importFingerprint) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "标注数据集标识");
        String sample = ValueUtils.requireNotBlank(sampleBatchId, "采样批次标识");
        String fingerprint = ValueUtils.requireNotBlank(
                importFingerprint, "标注导入指纹");
        if (!tableGateway.tableExists(tableName)) {
            return false;
        }
        // 导入指纹保存在稳定 JSON 中；该低频防重查询只读取数据集对应分区的 JSON 列。
        List<Row> rows = tableGateway.read(tableName,
                        Collections.singletonList("annotation_json"),
                        functions.col("dataset_id").equalTo(dataset)
                                .and(functions.col("sample_batch_id")
                                        .equalTo(sample)))
                .collectAsList();
        for (Row row : rows) {
            Map<String, Object> json = FmdbJsonCodec.readObject(
                    row.getAs("annotation_json"));
            if (fingerprint.equals(json.get("importFingerprint"))) {
                return true;
            }
        }
        return false;
    }

    private List<Row> readRows(org.apache.spark.sql.Column condition) {
        return tableGateway.read(tableName,
                        FmdbTableSchemas.columns(
                                FmdbPhysicalTable.ANNOTATION_RECORD), condition)
                .orderBy(functions.col("row_id")).collectAsList();
    }

    private static AnnotationBatch fromRows(List<Row> rows) {
        List<AnnotationRecord> records = new ArrayList<AnnotationRecord>(
                rows.size());
        for (Row row : rows) {
            records.add(fromRow(row));
        }
        AnnotationRecord first = records.get(0);
        return new AnnotationBatch(first.getAnnotationBatchId(),
                first.getSampleBatchId(), first.getDatasetId(),
                first.getBatchStatus(), first.getAnnotatedAt(),
                first.getPartitionMonth(), first.getSupersedesBatchId(), records);
    }

    private static FmdbTableRecord toTableRecord(AnnotationRecord record) {
        RowAnnotation annotation = record.getAnnotation();
        Map<String, Object> cellLabels = new LinkedHashMap<String, Object>();
        int index = 0;
        for (String column : annotation.getReviewedColumns()) {
            cellLabels.put(column, annotation.getCellLabels().get(index++).getLabel());
        }
        Map<String, Object> annotationJson = new LinkedHashMap<String, Object>();
        annotationJson.put("cellLabels", cellLabels);
        annotationJson.put("comment", annotation.getComment());
        annotationJson.put("errorColumns", annotation.getErrorColumns());
        annotationJson.put("importFingerprint", record.getImportFingerprint());
        annotationJson.put("reviewedColumns", annotation.getReviewedColumns());
        annotationJson.put("rowLabel", annotation.getRowLabel());
        annotationJson.put("sourceSnapshotId", annotation.getSourceSnapshotId());

        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("annotation_batch_id", record.getAnnotationBatchId());
        values.put("sample_batch_id", record.getSampleBatchId());
        values.put("dataset_id", record.getDatasetId());
        values.put("annotation_task_id", annotation.getAnnotationTaskId());
        values.put("row_id", annotation.getRowId());
        values.put("row_content_hash", annotation.getRowContentHash());
        values.put("row_data_json", FmdbJsonCodec.write(record.getRowData()));
        values.put("template_version", record.getTemplateVersion());
        values.put("file_name", record.getFileName());
        values.put("schema_hash", record.getSchemaHash());
        values.put("annotation_json", FmdbJsonCodec.write(annotationJson));
        values.put("annotator", record.getAnnotator());
        values.put("batch_status", record.getBatchStatus().name());
        values.put("batch_record_count", record.getBatchRecordCount());
        values.put("valid_record_count", record.getValidRecordCount());
        values.put("invalid_record_count", record.getInvalidRecordCount());
        values.put("supersedes_batch_id", record.getSupersedesBatchId());
        values.put("annotated_at", record.getAnnotatedAt());
        values.put("partition_month", record.getPartitionMonth());
        return FmdbTableRecord.of(FmdbPhysicalTable.ANNOTATION_RECORD, values);
    }

    @SuppressWarnings("unchecked")
    private static AnnotationRecord fromRow(Row row) {
        Map<String, Object> json = FmdbJsonCodec.readObject(
                row.getAs("annotation_json"));
        Set<String> reviewed = textSet(json.get("reviewedColumns"));
        Set<String> errors = textSet(json.get("errorColumns"));
        Object rawLabels = json.get("cellLabels");
        if (!(rawLabels instanceof Map)) {
            throw new IllegalArgumentException("标注 JSON 缺少 cellLabels");
        }
        Map<String, Object> labelValues = (Map<String, Object>) rawLabels;
        String datasetId = row.getAs("dataset_id");
        String snapshotId = requiredText(json, "sourceSnapshotId");
        String rowId = row.getAs("row_id");
        String annotator = row.getAs("annotator");
        long annotatedAt = ((Number) row.getAs("annotated_at")).longValue();
        List<CellLabel> labels = new ArrayList<CellLabel>();
        for (String column : reviewed) {
            Object label = labelValues.get(column);
            if (!(label instanceof Number)) {
                throw new IllegalArgumentException("标注 JSON 字段标签无效：" + column);
            }
            String cellId = new CellCoordinate(datasetId, snapshotId,
                    rowId, column).toCellId();
            labels.add(new CellLabel(cellId, ((Number) label).intValue(),
                    LabelSource.HUMAN, 1.0d, null, null,
                    annotator, annotatedAt));
        }
        RowAnnotation annotation = new RowAnnotation(
                row.getAs("annotation_task_id"), rowId, snapshotId,
                row.getAs("row_content_hash"),
                ((Number) json.get("rowLabel")).intValue(), reviewed, errors,
                (String) json.get("comment"), labels);
        return new AnnotationRecord(row.getAs("annotation_batch_id"),
                row.getAs("sample_batch_id"), datasetId, annotation,
                FmdbJsonCodec.readObject(row.getAs("row_data_json")),
                row.getAs("template_version"), row.getAs("file_name"),
                row.getAs("schema_hash"), annotator,
                AnnotationBatchStatus.valueOf((String) row.getAs("batch_status")),
                ((Number) row.getAs("batch_record_count")).longValue(),
                ((Number) row.getAs("valid_record_count")).longValue(),
                ((Number) row.getAs("invalid_record_count")).longValue(),
                row.getAs("supersedes_batch_id"), annotatedAt,
                row.getAs("partition_month"),
                requiredText(json, "importFingerprint"));
    }

    @SuppressWarnings("unchecked")
    private static Set<String> textSet(Object raw) {
        if (!(raw instanceof List)) {
            throw new IllegalArgumentException("标注 JSON 字段列表无效");
        }
        Set<String> result = new LinkedHashSet<String>();
        for (Object value : (List<Object>) raw) {
            result.add(String.valueOf(value));
        }
        return result;
    }

    private static String requiredText(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof String) || ((String) value).trim().isEmpty()) {
            throw new IllegalArgumentException("标注 JSON 缺少字段：" + key);
        }
        return (String) value;
    }
}
