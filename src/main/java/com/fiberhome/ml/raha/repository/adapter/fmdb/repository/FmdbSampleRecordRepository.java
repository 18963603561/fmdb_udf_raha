package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.loader.identity.RowFingerprintAlgorithm;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityMode;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbRawValueAccessPolicy;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用最终采样宽表幂等追加和分区裁剪读取完整 c1 批次。
 */
public final class FmdbSampleRecordRepository implements SampleRecordRepository {

    /** 标注展示热路径只读取的最小字段集合。 */
    private static final List<String> ANNOTATION_COLUMNS =
            Collections.unmodifiableList(Arrays.asList(
                    "row_id", "row_content_hash", "schema_hash",
                    "column_schema_json", "row_data_json",
                    "sampling_context_json"));

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbSampleRecordRepository.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 统一持久化配置。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 采样物理表名。 */
    private final String tableName;
    /** 原始 c1 行访问策略。 */
    private final FmdbRawValueAccessPolicy rawValueAccessPolicy;

    public FmdbSampleRecordRepository(SparkSession sparkSession,
                                      FmdbTableGateway tableGateway,
                                      FmdbPersistenceConfig persistenceConfig) {
        this(sparkSession, tableGateway, persistenceConfig,
                FmdbPhysicalTable.SAMPLE_RECORD.getTableName(),
                FmdbRawValueAccessPolicy.allowAllInternal());
    }

    public FmdbSampleRecordRepository(SparkSession sparkSession,
                                      FmdbTableGateway tableGateway,
                                      FmdbPersistenceConfig persistenceConfig,
                                      String tableName) {
        this(sparkSession, tableGateway, persistenceConfig, tableName,
                FmdbRawValueAccessPolicy.allowAllInternal());
    }

    public FmdbSampleRecordRepository(SparkSession sparkSession,
                                      FmdbTableGateway tableGateway,
                                      FmdbPersistenceConfig persistenceConfig,
                                      String tableName,
                                      FmdbRawValueAccessPolicy rawValueAccessPolicy) {
        if (sparkSession == null || tableGateway == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 采样仓储依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = ValueUtils.requireNotBlank(tableName, "FMDB 采样表名");
        if (rawValueAccessPolicy == null) {
            throw new IllegalArgumentException("原始 c1 行访问策略不能为空");
        }
        this.rawValueAccessPolicy = rawValueAccessPolicy;
    }

    @Override
    public boolean isPersistenceEnabled() {
        return persistenceConfig.shouldPersist(FmdbPhysicalTable.SAMPLE_RECORD);
    }

    @Override
    public long saveAll(SampleBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("FMDB 采样批次不能为空");
        }
        if (!isPersistenceEnabled()) {
            LOGGER.info("FMDB 采样记录入库已关闭，跳过写入，sampleBatchId={}，"
                            + "recordCount={}，configKey={}",
                    batch.getSampleBatchId(), batch.getRecords().size(),
                    FmdbPhysicalTable.SAMPLE_RECORD.getConfigKey());
            return 0L;
        }
        List<Row> rows = new ArrayList<Row>(batch.getRecords().size());
        for (SampleRecord record : batch.getRecords()) {
            rows.add(toTableRecord(record).toRow());
        }
        Dataset<Row> frame = sparkSession.createDataFrame(rows,
                FmdbTableSchemas.schema(FmdbPhysicalTable.SAMPLE_RECORD));
        LOGGER.info("开始追加 FMDB 采样记录，sampleBatchId={}，recordCount={}，"
                        + "tableName={}", batch.getSampleBatchId(), rows.size(),
                tableName);
        try {
            long written = tableGateway.appendIdempotent(tableName, frame,
                    Arrays.asList("sample_batch_id", "row_id"));
            LOGGER.info("FMDB 采样记录追加完成，sampleBatchId={}，inputCount={}，"
                            + "writtenCount={}，skippedCount={}",
                    batch.getSampleBatchId(), rows.size(), written,
                    rows.size() - written);
            return written;
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 采样记录追加失败，sampleBatchId={}，recordCount={}，"
                            + "tableName={}", batch.getSampleBatchId(), rows.size(),
                    tableName, exception);
            throw exception;
        }
    }

    @Override
    public Optional<SampleBatch> find(String datasetId,
                                      String partitionMonth,
                                      String sampleBatchId) {
        String validatedDataset = ValueUtils.requireNotBlank(
                datasetId, "采样查询数据集标识");
        String validatedMonth = ValueUtils.requireNotBlank(
                partitionMonth, "采样查询月分区");
        String validatedBatch = ValueUtils.requireNotBlank(
                sampleBatchId, "采样查询批次标识");
        rawValueAccessPolicy.requireRead(validatedDataset, "读取完整 c1 批次");
        if (!tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        LOGGER.debug("开始分区裁剪读取 FMDB 采样批次，datasetId={}，"
                        + "partitionMonth={}，sampleBatchId={}，tableName={}",
                validatedDataset, validatedMonth, validatedBatch, tableName);
        try {
            Dataset<Row> selected = tableGateway.read(tableName,
                    FmdbTableSchemas.columns(FmdbPhysicalTable.SAMPLE_RECORD),
                    functions.col("dataset_id").equalTo(validatedDataset)
                            .and(functions.col("partition_month")
                                    .equalTo(validatedMonth))
                            .and(functions.col("sample_batch_id")
                                    .equalTo(validatedBatch)));
            List<Row> rows = selected.orderBy(functions.col("row_id")).collectAsList();
            if (rows.isEmpty()) {
                return Optional.empty();
            }
            List<SampleRecord> records = new ArrayList<SampleRecord>(rows.size());
            for (Row row : rows) {
                records.add(fromRow(row));
            }
            SampleRecord first = records.get(0);
            String snapshotId = String.valueOf(
                    first.getSamplingContext().get("snapshotId"));
            SampleBatch batch = new SampleBatch(validatedBatch, validatedDataset,
                    snapshotId, first.getSourceVersion(), first.getSamplingVersion(),
                    first.getCreatedAt(), validatedMonth, records);
            LOGGER.debug("FMDB 采样批次读取完成，sampleBatchId={}，recordCount={}",
                    validatedBatch, records.size());
            return Optional.of(batch);
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 采样批次读取失败，datasetId={}，partitionMonth={}，"
                            + "sampleBatchId={}，tableName={}", validatedDataset,
                    validatedMonth, validatedBatch, tableName, exception);
            throw exception;
        }
    }

    @Override
    public Optional<SampleBatch> findByBatchId(String sampleBatchId) {
        String batchId = ValueUtils.requireNotBlank(
                sampleBatchId, "全局采样批次标识");
        if (!tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        LOGGER.debug("开始定位 FMDB 全局采样批次，sampleBatchId={}", batchId);
        List<Row> locations = tableGateway.read(tableName,
                Arrays.asList("dataset_id", "partition_month"),
                functions.col("sample_batch_id").equalTo(batchId))
                .distinct().collectAsList();
        if (locations.isEmpty()) {
            return Optional.empty();
        }
        if (locations.size() != 1) {
            throw new IllegalStateException("采样批次标识不是全局唯一：" + batchId);
        }
        Row location = locations.get(0);
        String datasetId = location.getAs("dataset_id");
        String partitionMonth = location.getAs("partition_month");
        LOGGER.debug("FMDB 全局采样批次定位完成，sampleBatchId={}，datasetId={}，"
                        + "partitionMonth={}", batchId, datasetId, partitionMonth);
        return find(datasetId, partitionMonth, batchId);
    }

    @Override
    public List<SampleAnnotationRow> findForAnnotation(
            String datasetId,
            String partitionMonth,
            String sampleBatchId) {
        String validatedDataset = ValueUtils.requireNotBlank(
                datasetId, "标注查询数据集标识");
        String validatedMonth = ValueUtils.requireNotBlank(
                partitionMonth, "标注查询月分区");
        String validatedBatch = ValueUtils.requireNotBlank(
                sampleBatchId, "标注查询采样批次");
        rawValueAccessPolicy.requireRead(validatedDataset, "导出标注原始行");
        if (!tableGateway.tableExists(tableName)) {
            return Collections.emptyList();
        }
        LOGGER.debug("开始列裁剪读取 FMDB 标注展示行，datasetId={}，"
                        + "partitionMonth={}，sampleBatchId={}，columnCount={}",
                validatedDataset, validatedMonth, validatedBatch,
                ANNOTATION_COLUMNS.size());
        Dataset<Row> selected = tableGateway.read(tableName, ANNOTATION_COLUMNS,
                functions.col("dataset_id").equalTo(validatedDataset)
                        .and(functions.col("partition_month")
                                .equalTo(validatedMonth))
                        .and(functions.col("sample_batch_id")
                                .equalTo(validatedBatch)));
        List<SampleAnnotationRow> result = new ArrayList<SampleAnnotationRow>();
        for (Row row : selected.orderBy(functions.col("row_id")).collectAsList()) {
            Map<String, Object> context = FmdbJsonCodec.readObject(
                    row.getAs("sampling_context_json"));
            result.add(new SampleAnnotationRow(String.valueOf(
                    context.get("annotationTaskId")), row.getAs("row_id"),
                    row.getAs("row_content_hash"), row.getAs("schema_hash"),
                    FmdbJsonCodec.readObject(row.getAs("column_schema_json")),
                    FmdbJsonCodec.readObject(row.getAs("row_data_json")), context));
        }
        LOGGER.debug("FMDB 标注展示行读取完成，sampleBatchId={}，recordCount={}",
                validatedBatch, result.size());
        return Collections.unmodifiableList(result);
    }

    private static FmdbTableRecord toTableRecord(SampleRecord record) {
        Map<String, Object> keyColumns = new LinkedHashMap<String, Object>();
        keyColumns.put("columns", record.getRowKeyColumns());
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("sample_batch_id", record.getSampleBatchId());
        values.put("dataset_id", record.getDatasetId());
        values.put("input_reference", record.getInputReference());
        values.put("source_version", record.getSourceVersion());
        values.put("row_identity_mode", record.getRowIdentityMode().name());
        values.put("row_key_columns_json", FmdbJsonCodec.write(keyColumns));
        values.put("row_fingerprint_algorithm",
                record.getFingerprintAlgorithm().getStandardName());
        values.put("row_fingerprint_version", record.getFingerprintVersion());
        values.put("row_id", record.getRowId());
        values.put("row_content_hash", record.getRowContentHash());
        values.put("schema_hash", record.getSchemaHash());
        values.put("column_schema_json", FmdbJsonCodec.write(
                record.getColumnSchema()));
        values.put("row_data_json", FmdbJsonCodec.write(record.getRowData()));
        values.put("duplicate_count", record.getDuplicateCount());
        values.put("sampling_version", record.getSamplingVersion());
        values.put("sampling_context_json", FmdbJsonCodec.write(
                record.getSamplingContext()));
        values.put("created_at", record.getCreatedAt());
        values.put("partition_month", record.getPartitionMonth());
        return FmdbTableRecord.of(FmdbPhysicalTable.SAMPLE_RECORD, values);
    }

    @SuppressWarnings("unchecked")
    private static SampleRecord fromRow(Row row) {
        Map<String, Object> keyObject = FmdbJsonCodec.readObject(
                row.getAs("row_key_columns_json"));
        Object keyValue = keyObject.get("columns");
        List<String> keyColumns = new ArrayList<String>();
        if (keyValue instanceof List) {
            for (Object value : (List<Object>) keyValue) {
                keyColumns.add(String.valueOf(value));
            }
        }
        return new SampleRecord(
                row.getAs("sample_batch_id"), row.getAs("dataset_id"),
                row.getAs("input_reference"), row.getAs("source_version"),
                RowIdentityMode.valueOf((String) row.getAs("row_identity_mode")),
                keyColumns, fingerprintAlgorithm((String) row.getAs(
                "row_fingerprint_algorithm")),
                row.getAs("row_fingerprint_version"), row.getAs("row_id"),
                row.getAs("row_content_hash"), row.getAs("schema_hash"),
                FmdbJsonCodec.readObject(row.getAs("column_schema_json")),
                FmdbJsonCodec.readObject(row.getAs("row_data_json")),
                ((Number) row.getAs("duplicate_count")).longValue(),
                row.getAs("sampling_version"), FmdbJsonCodec.readObject(
                row.getAs("sampling_context_json")),
                ((Number) row.getAs("created_at")).longValue(),
                row.getAs("partition_month"));
    }

    private static RowFingerprintAlgorithm fingerprintAlgorithm(String value) {
        for (RowFingerprintAlgorithm algorithm : RowFingerprintAlgorithm.values()) {
            if (algorithm.getStandardName().equals(value)) {
                return algorithm;
            }
        }
        throw new IllegalArgumentException("不支持的采样行指纹算法：" + value);
    }
}
