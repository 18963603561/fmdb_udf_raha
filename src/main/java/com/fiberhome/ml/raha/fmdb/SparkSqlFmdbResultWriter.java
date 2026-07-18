package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.job.RahaJob;
import com.fiberhome.ml.raha.util.FormDataCodec;
import com.fiberhome.ml.raha.util.ValueUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 将 Raha 任务状态和检测明细转换为稳定 FMDB 表模式后幂等写入。
 */
public final class SparkSqlFmdbResultWriter implements FmdbResultWriter {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SparkSqlFmdbResultWriter.class);
    /** 任务状态表模式。 */
    private static final StructType JOB_SCHEMA = DataTypes.createStructType(
            new StructField[]{
                    field("job_id", DataTypes.StringType, false),
                    field("idempotent_key", DataTypes.StringType, false),
                    field("job_type", DataTypes.StringType, false),
                    field("dataset_id", DataTypes.StringType, false),
                    field("snapshot_id", DataTypes.StringType, true),
                    field("config_version", DataTypes.StringType, false),
                    field("status", DataTypes.StringType, false),
                    field("created_at", DataTypes.LongType, false),
                    field("started_at", DataTypes.LongType, false),
                    field("finished_at", DataTypes.LongType, false),
                    field("error_code", DataTypes.StringType, true),
                    field("error_message", DataTypes.StringType, true),
                    field("written_at", DataTypes.LongType, false)
            });
    /** 检测结果表模式。 */
    private static final StructType DETECTION_SCHEMA = DataTypes.createStructType(
            new StructField[]{
                    field("job_id", DataTypes.StringType, false),
                    field("config_version", DataTypes.StringType, false),
                    field("stage_id", DataTypes.StringType, false),
                    field("dataset_id", DataTypes.StringType, false),
                    field("snapshot_id", DataTypes.StringType, false),
                    field("row_id", DataTypes.StringType, false),
                    field("column_name", DataTypes.StringType, false),
                    field("cell_id", DataTypes.StringType, false),
                    field("value_hash", DataTypes.StringType, false),
                    field("masked_value", DataTypes.StringType, true),
                    field("is_error", DataTypes.BooleanType, false),
                    field("score", DataTypes.DoubleType, false),
                    field("threshold", DataTypes.DoubleType, false),
                    field("strategy_ids", DataTypes.StringType, false),
                    field("reasons", DataTypes.StringType, false),
                    field("model_name", DataTypes.StringType, false),
                    field("model_version", DataTypes.StringType, false),
                    field("feature_dictionary_version", DataTypes.StringType, false),
                    field("detected_at", DataTypes.LongType, false),
                    field("written_at", DataTypes.LongType, false)
            });
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 提供可测试写入时间的时钟。 */
    private final Clock clock;
    public SparkSqlFmdbResultWriter(SparkSession sparkSession,
                                    FmdbTableGateway tableGateway,
                                    Clock clock) {
        if (sparkSession == null || tableGateway == null || clock == null) {
            throw new IllegalArgumentException("FMDB 结果写入器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.clock = clock;
    }

    @Override
    public long writeJob(String tableName, RahaJob job) {
        if (job == null) {
            throw new IllegalArgumentException("FMDB 任务记录不能为空");
        }
        LOGGER.info("开始写入 FMDB 任务状态，jobId={}，status={}，tableName={}",
                job.getJobId(), job.getStatus(), tableName);
        Row row = RowFactory.create(job.getJobId(), job.getIdempotentKey(),
                job.getJobType().name(), job.getDatasetId(), job.getSnapshotId(),
                job.getConfigVersion(), job.getStatus().name(),
                job.getCreatedAt(), job.getStartedAt(), job.getFinishedAt(),
                job.getErrorCode(), job.getErrorMessage(), positiveNow());
        Dataset<Row> frame = sparkSession.createDataFrame(
                Collections.singletonList(row), JOB_SCHEMA);
        return tableGateway.appendIdempotent(tableName, frame,
                Arrays.asList("job_id", "status"));
    }

    @Override
    public long writeDetectionResults(String tableName,
                                      String jobId,
                                      List<DetectionResult> results) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "FMDB 检测任务标识");
        if (results == null) {
            throw new IllegalArgumentException("FMDB 检测结果集合不能为空");
        }
        List<Row> rows = new ArrayList<Row>(results.size());
        long writtenAt = positiveNow();
        for (DetectionResult result : results) {
            if (result == null || !validatedJobId.equals(result.getJobId())) {
                throw new IllegalArgumentException("FMDB 检测结果任务标识不一致");
            }
            rows.add(RowFactory.create(result.getJobId(), result.getConfigVersion(),
                    result.getStageId(), result.getCoordinate().getDatasetId(),
                    result.getCoordinate().getSnapshotId(),
                    result.getCoordinate().getRowId(),
                    result.getCoordinate().getColumnName(),
                    result.getCoordinate().toCellId(), result.getValueHash(),
                    null,
                    result.isError(), result.getScore(),
                    result.getThreshold(), FormDataCodec.encodeList(result.getStrategyIds()),
                    FormDataCodec.encode(result.getReasons()), result.getModelName(),
                    result.getModelVersion(), result.getFeatureDictionaryVersion(),
                    result.getDetectedAt(), writtenAt));
        }
        if (rows.isEmpty()) {
            return 0L;
        }
        LOGGER.info("开始写入 FMDB 检测结果，jobId={}，resultCount={}，tableName={}",
                validatedJobId, rows.size(), tableName);
        Dataset<Row> frame = sparkSession.createDataFrame(rows, DETECTION_SCHEMA);
        long count = tableGateway.appendIdempotent(tableName, frame,
                Arrays.asList("job_id", "cell_id", "model_version"));
        LOGGER.info("FMDB 检测结果写入完成，jobId={}，writtenCount={}",
                validatedJobId, count);
        return count;
    }

    private long positiveNow() {
        return Math.max(1L, clock.millis());
    }

    private static StructField field(String name,
                                     org.apache.spark.sql.types.DataType type,
                                     boolean nullable) {
        return DataTypes.createStructField(name, type, nullable);
    }
}
