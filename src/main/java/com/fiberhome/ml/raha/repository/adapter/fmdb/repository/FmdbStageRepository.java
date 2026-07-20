package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.type.StageStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.job.domain.RahaStage;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPartitionUtils;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用阶段尝试表追加状态快照，并按状态版本恢复每次阶段尝试的最新状态。
 */
public final class FmdbStageRepository implements StageRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbStageRepository.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 阶段尝试表名。 */
    private final String stageTable;
    /** 任务状态表名。 */
    private final String jobTable;

    public FmdbStageRepository(SparkSession sparkSession,
                               FmdbTableGateway tableGateway,
                               FmdbPersistenceConfig persistenceConfig) {
        if (sparkSession == null || tableGateway == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 阶段仓储依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.stageTable = FmdbPhysicalTable.JOB_STAGE_ATTEMPT.getTableName();
        this.jobTable = FmdbPhysicalTable.JOB_RUN.getTableName();
    }

    @Override
    public synchronized SaveOutcome save(RahaStage stage,
                                         ArtifactVersion version,
                                         long updatedAt) {
        if (stage == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("阶段、版本和更新时间必须有效");
        }
        if (!persistenceConfig.shouldPersist(
                FmdbPhysicalTable.JOB_STAGE_ATTEMPT)) {
            throw new IllegalStateException("FMDB 阶段状态持久化已关闭");
        }
        JobContext job = requireJobContext(stage.getJobId());
        Row latest = latestState(stage.getJobId(), stage.getStageId(),
                stage.getAttemptId());
        if (latest != null && sameState(latest, stage, version)) {
            return SaveOutcome.UNCHANGED;
        }
        int stateVersion = latest == null ? 1
                : Math.addExact(((Number) latest.getAs("state_version")).intValue(), 1);
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("job_id", stage.getJobId());
        values.put("dataset_id", job.datasetId);
        values.put("stage_id", stage.getStageId());
        values.put("stage_type", stage.getStageType().name());
        values.put("attempt_id", stage.getAttemptId());
        values.put("state_version", stateVersion);
        values.put("checkpoint_id", null);
        values.put("status", stage.getStatus().name());
        values.put("input_version_json", FmdbJsonCodec.write(version));
        values.put("input_fingerprint", null);
        values.put("output_location", null);
        values.put("summary_json", null);
        values.put("error_code", stage.getErrorCode());
        values.put("error_message", stage.getErrorMessage());
        values.put("started_at", stage.getStartedAt());
        values.put("completed_at", stage.getFinishedAt());
        values.put("updated_at", updatedAt);
        values.put("partition_month", FmdbPartitionUtils.month(job.createdAt));
        Row row = FmdbTableRecord.of(FmdbPhysicalTable.JOB_STAGE_ATTEMPT,
                values).toRow();
        Dataset<Row> frame = sparkSession.createDataFrame(
                Collections.singletonList(row),
                FmdbTableSchemas.schema(FmdbPhysicalTable.JOB_STAGE_ATTEMPT));
        LOGGER.info("开始写入 FMDB 阶段状态，jobId={}，stageId={}，attemptId={}，"
                        + "stateVersion={}，status={}", stage.getJobId(), stage.getStageId(),
                stage.getAttemptId(), stateVersion, stage.getStatus());
        try {
            long written = tableGateway.append(stageTable, frame,
                    Arrays.asList("job_id", "stage_id", "attempt_id", "state_version"), 1L);
            if (written != 1L) {
                throw new IllegalStateException("FMDB 阶段状态写入数量异常：" + written);
            }
            return latest == null ? SaveOutcome.CREATED : SaveOutcome.UPDATED;
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 阶段状态写入失败，jobId={}，stageId={}，attemptId={}",
                    stage.getJobId(), stage.getStageId(), stage.getAttemptId(), exception);
            throw exception;
        }
    }

    @Override
    public List<RahaStage> findByJobId(String jobId) {
        String validated = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.JOB_STAGE_ATTEMPT)
                || !tableGateway.tableExists(stageTable)) {
            return Collections.emptyList();
        }
        LOGGER.debug("开始从 FMDB 恢复阶段状态，jobId={}", validated);
        List<Row> rows = tableGateway.read(stageTable,
                Arrays.asList("job_id", "stage_id", "stage_type", "attempt_id",
                        "state_version", "status", "error_code", "error_message",
                        "started_at", "completed_at"),
                functions.col("job_id").equalTo(validated)).collectAsList();
        Map<String, Row> latest = new LinkedHashMap<String, Row>();
        for (Row row : rows) {
            String key = row.getAs("stage_id") + ":" + row.getAs("attempt_id");
            Row previous = latest.get(key);
            if (previous == null || number(row, "state_version")
                    > number(previous, "state_version")) {
                latest.put(key, row);
            }
        }
        List<RahaStage> stages = new ArrayList<RahaStage>(latest.size());
        for (Row row : latest.values()) {
            stages.add(RahaStage.restore((String) row.getAs("stage_id"),
                    (String) row.getAs("job_id"),
                    StageType.valueOf((String) row.getAs("stage_type")),
                    ((Number) row.getAs("attempt_id")).intValue(),
                    StageStatus.valueOf((String) row.getAs("status")),
                    number(row, "started_at"), number(row, "completed_at"),
                    (String) row.getAs("error_code"),
                    (String) row.getAs("error_message")));
        }
        Collections.sort(stages, Comparator.comparingLong(RahaStage::getStartedAt)
                .thenComparing(RahaStage::getStageId)
                .thenComparingInt(RahaStage::getAttemptId));
        return Collections.unmodifiableList(stages);
    }

    private JobContext requireJobContext(String jobId) {
        if (!tableGateway.tableExists(jobTable)) {
            throw new IllegalStateException("FMDB 阶段状态缺少任务主记录：" + jobId);
        }
        List<Row> rows = tableGateway.read(jobTable,
                Arrays.asList("dataset_id", "created_at", "state_version"),
                functions.col("job_id").equalTo(jobId))
                .orderBy(functions.col("state_version").desc()).limit(1).collectAsList();
        if (rows.isEmpty()) {
            throw new IllegalStateException("FMDB 阶段状态缺少任务上下文：" + jobId);
        }
        return new JobContext((String) rows.get(0).getAs("dataset_id"),
                number(rows.get(0), "created_at"));
    }

    private Row latestState(String jobId, String stageId, int attemptId) {
        if (!tableGateway.tableExists(stageTable)) {
            return null;
        }
        List<Row> rows = tableGateway.read(stageTable,
                Arrays.asList("state_version", "status", "input_version_json",
                        "error_code", "error_message", "started_at", "completed_at"),
                functions.col("job_id").equalTo(jobId)
                        .and(functions.col("stage_id").equalTo(stageId))
                        .and(functions.col("attempt_id").equalTo(attemptId)))
                .orderBy(functions.col("state_version").desc()).limit(1).collectAsList();
        return rows.isEmpty() ? null : rows.get(0);
    }

    private static boolean sameState(Row row,
                                     RahaStage stage,
                                     ArtifactVersion version) {
        return stage.getStatus().name().equals(row.getAs("status"))
                && FmdbJsonCodec.write(version).equals(row.getAs("input_version_json"))
                && equalsValue(stage.getErrorCode(), row.getAs("error_code"))
                && equalsValue(stage.getErrorMessage(), row.getAs("error_message"))
                && stage.getStartedAt() == number(row, "started_at")
                && stage.getFinishedAt() == number(row, "completed_at");
    }

    private static boolean equalsValue(Object first, Object second) {
        return first == null ? second == null : first.equals(second);
    }

    private static long number(Row row, String column) {
        return ((Number) row.getAs(column)).longValue();
    }

    /** 阶段物理分区所需的任务上下文。 */
    private static final class JobContext {

        /** 数据集标识。 */
        private final String datasetId;
        /** 任务创建时间。 */
        private final long createdAt;

        private JobContext(String datasetId, long createdAt) {
            this.datasetId = datasetId;
            this.createdAt = createdAt;
        }
    }
}
