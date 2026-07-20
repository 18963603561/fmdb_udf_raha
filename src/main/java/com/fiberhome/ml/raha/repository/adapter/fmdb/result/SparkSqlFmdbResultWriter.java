package com.fiberhome.ml.raha.repository.adapter.fmdb.result;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPartitionUtils;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbRawValueAccessPolicy;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按最终 FMDB 九表协议追加任务状态和错误检测结果。
 */
public final class SparkSqlFmdbResultWriter implements FmdbResultWriter {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SparkSqlFmdbResultWriter.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 提供可测试写入时间的时钟。 */
    private final Clock clock;
    /** 统一持久化配置。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 检测原始值写入策略。 */
    private final FmdbRawValueAccessPolicy rawValueAccessPolicy;

    public SparkSqlFmdbResultWriter(SparkSession sparkSession,
                                    FmdbTableGateway tableGateway,
                                    Clock clock,
                                    FmdbPersistenceConfig persistenceConfig) {
        this(sparkSession, tableGateway, clock, persistenceConfig,
                FmdbRawValueAccessPolicy.allowAllInternal());
    }

    public SparkSqlFmdbResultWriter(SparkSession sparkSession,
                                    FmdbTableGateway tableGateway,
                                    Clock clock,
                                    FmdbPersistenceConfig persistenceConfig,
                                    FmdbRawValueAccessPolicy rawValueAccessPolicy) {
        if (sparkSession == null || tableGateway == null || clock == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 结果写入器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.clock = clock;
        this.persistenceConfig = persistenceConfig;
        if (rawValueAccessPolicy == null) {
            throw new IllegalArgumentException("检测原始值访问策略不能为空");
        }
        this.rawValueAccessPolicy = rawValueAccessPolicy;
    }

    @Override
    public synchronized long writeJob(String tableName,
                                      RahaJob job,
                                      Map<String, Object> resultSummary) {
        if (job == null) {
            throw new IllegalArgumentException("FMDB 任务记录不能为空");
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.JOB_RUN)) {
            LOGGER.info("FMDB 任务状态入库已关闭，跳过写入，jobId={}，configKey={}",
                    job.getJobId(), FmdbPhysicalTable.JOB_RUN.getConfigKey());
            return 0L;
        }
        Row latest = latestJobState(tableName, job);
        String summaryJson = FmdbJsonCodec.write(resultSummary);
        if (latest != null && sameJobState(latest, job, summaryJson)) {
            LOGGER.info("FMDB 任务状态未变化，跳过重复快照，jobId={}，status={}",
                    job.getJobId(), job.getStatus());
            return 0L;
        }
        int stateVersion = latest == null ? 1
                : Math.addExact((Integer) latest.getAs("state_version"), 1);
        long updatedAt = positiveNow();
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("job_id", job.getJobId());
        values.put("state_version", stateVersion);
        values.put("dataset_id", job.getDatasetId());
        values.put("idempotent_key", job.getIdempotentKey());
        values.put("job_type", job.getJobType().name());
        values.put("snapshot_id", job.getSnapshotId());
        values.put("config_version", job.getConfigVersion());
        values.put("status", job.getStatus().name());
        values.put("current_stage_id", job.getCurrentStageId());
        values.put("result_summary_json", summaryJson);
        values.put("error_code", job.getErrorCode());
        values.put("error_message", job.getErrorMessage());
        values.put("created_at", job.getCreatedAt());
        values.put("started_at", job.getStartedAt());
        values.put("finished_at", job.getFinishedAt());
        values.put("updated_at", updatedAt);
        values.put("partition_month", FmdbPartitionUtils.month(job.getCreatedAt()));
        Dataset<Row> frame = frame(FmdbPhysicalTable.JOB_RUN,
                Collections.singletonList(FmdbTableRecord.of(
                        FmdbPhysicalTable.JOB_RUN, values)));
        LOGGER.info("开始写入 FMDB 任务状态，jobId={}，stateVersion={}，status={}，"
                        + "tableName={}", job.getJobId(), stateVersion,
                job.getStatus(), tableName);
        return tableGateway.append(tableName, frame,
                Arrays.asList("job_id", "state_version"), 1L);
    }

    @Override
    public long writeDetectionResults(String tableName,
                                      FmdbDetectionWriteContext context,
                                      List<DetectionResult> results) {
        if (context == null || results == null) {
            throw new IllegalArgumentException("FMDB 检测上下文和结果不能为空");
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.DETECTION_RESULT)) {
            LOGGER.info("FMDB 检测结果入库已关闭，跳过写入，detectionBatchId={}，"
                            + "resultCount={}，configKey={}",
                    context.getDetectionBatchId(), results.size(),
                    FmdbPhysicalTable.DETECTION_RESULT.getConfigKey());
            return 0L;
        }
        if (!results.isEmpty()) {
            rawValueAccessPolicy.requireRead(
                    results.get(0).getCoordinate().getDatasetId(), "写入检测原始值");
        }
        List<FmdbTableRecord> records = new ArrayList<FmdbTableRecord>();
        for (DetectionResult result : results) {
            if (result == null) {
                throw new IllegalArgumentException("FMDB 检测结果不能包含空值");
            }
            // 最终物理表只保存错误单元格，正常预测仅参与任务指标。
            if (!result.isError()) {
                continue;
            }
            Map<String, Object> sourceRow = context.requireRow(
                    result.getCoordinate().getRowId());
            String columnName = result.getCoordinate().getColumnName();
            Map<String, Object> reason = new LinkedHashMap<String, Object>();
            reason.put("configVersion", result.getConfigVersion());
            reason.put("featureDictionaryVersion",
                    result.getFeatureDictionaryVersion());
            reason.put("maskedValue", result.getMaskedValue());
            reason.put("modelName", result.getModelName());
            reason.put("reasons", result.getReasons());
            reason.put("snapshotId", result.getCoordinate().getSnapshotId());
            reason.put("stageId", result.getStageId());
            reason.put("strategyIds", result.getStrategyIds());
            reason.put("valueHash", result.getValueHash());
            Map<String, Object> values = new LinkedHashMap<String, Object>();
            values.put("detection_batch_id", context.getDetectionBatchId());
            values.put("dataset_id", result.getCoordinate().getDatasetId());
            values.put("input_reference", context.getInputReference());
            values.put("model_set_version", context.getModelSetVersion());
            values.put("model_version", result.getModelVersion());
            values.put("row_id", result.getCoordinate().getRowId());
            values.put("column_name", columnName);
            values.put("cell_id", result.getCoordinate().toCellId());
            Object original = sourceRow.get(columnName);
            values.put("original_value", original == null
                    ? null : String.valueOf(original));
            values.put("row_data_json", FmdbJsonCodec.write(sourceRow));
            values.put("score", result.getScore());
            values.put("threshold", result.getThreshold());
            values.put("error_reason_json", FmdbJsonCodec.write(reason));
            values.put("detected_at", result.getDetectedAt());
            values.put("partition_date", FmdbPartitionUtils.date(
                    result.getDetectedAt()));
            records.add(FmdbTableRecord.of(
                    FmdbPhysicalTable.DETECTION_RESULT, values));
        }
        if (records.isEmpty()) {
            LOGGER.info("检测批次没有错误结果，跳过错误表写入，detectionBatchId={}",
                    context.getDetectionBatchId());
            return 0L;
        }
        Dataset<Row> frame = frame(FmdbPhysicalTable.DETECTION_RESULT, records);
        // 检测结果明细固定直接追加，避免受全局写入模式影响而扫描历史结果主键。
        long count = tableGateway.appendDirect(tableName, frame, records.size());
        LOGGER.info("FMDB 错误结果直接追加完成，detectionBatchId={}，writtenCount={}",
                context.getDetectionBatchId(), count);
        return count;
    }

    private Dataset<Row> frame(FmdbPhysicalTable table,
                               List<FmdbTableRecord> records) {
        List<Row> rows = new ArrayList<Row>(records.size());
        for (FmdbTableRecord record : records) {
            if (record.getTable() != table) {
                throw new IllegalArgumentException("FMDB 批量记录所属表不一致");
            }
            rows.add(record.toRow());
        }
        return sparkSession.createDataFrame(rows, FmdbTableSchemas.schema(table));
    }

    private Row latestJobState(String tableName, RahaJob job) {
        if (!tableGateway.tableExists(tableName)) {
            return null;
        }
        List<Row> rows = tableGateway.read(tableName,
                        Arrays.asList("snapshot_id", "status", "current_stage_id",
                                "result_summary_json", "error_code", "error_message",
                                "started_at", "finished_at", "state_version"),
                        functions.col("job_id").equalTo(job.getJobId())
                                .and(functions.col("dataset_id")
                                        .equalTo(job.getDatasetId()))
                                .and(functions.col("partition_month")
                                        .equalTo(FmdbPartitionUtils.month(job.getCreatedAt()))))
                .orderBy(functions.col("state_version").desc())
                .limit(1).collectAsList();
        return rows.isEmpty() ? null : rows.get(0);
    }

    private static boolean sameJobState(Row latest,
                                        RahaJob job,
                                        String summaryJson) {
        return equalsValue(latest.getAs("snapshot_id"), job.getSnapshotId())
                && equalsValue(latest.getAs("status"), job.getStatus().name())
                && equalsValue(latest.getAs("current_stage_id"),
                job.getCurrentStageId())
                && equalsValue(latest.getAs("result_summary_json"), summaryJson)
                && equalsValue(latest.getAs("error_code"), job.getErrorCode())
                && equalsValue(latest.getAs("error_message"), job.getErrorMessage())
                && ((Long) latest.getAs("started_at")) == job.getStartedAt()
                && ((Long) latest.getAs("finished_at")) == job.getFinishedAt();
    }

    private static boolean equalsValue(Object first, Object second) {
        return first == null ? second == null : first.equals(second);
    }

    private long positiveNow() {
        return Math.max(1L, clock.millis());
    }
}
