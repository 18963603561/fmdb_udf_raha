package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.repository.port.JobRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 `raha_job_run` 追加状态快照并恢复最新任务状态的生产任务仓储。
 */
public final class FmdbJobRepository implements JobRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbJobRepository.class);
    /** FMDB 状态写入器。 */
    private final FmdbResultWriter resultWriter;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化配置。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 任务状态表名。 */
    private final String tableName;

    public FmdbJobRepository(FmdbResultWriter resultWriter,
                             FmdbTableGateway tableGateway,
                             FmdbPersistenceConfig persistenceConfig) {
        if (resultWriter == null || tableGateway == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 任务仓储依赖不能为空");
        }
        this.resultWriter = resultWriter;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = FmdbPhysicalTable.JOB_RUN.getTableName();
    }

    @Override
    public synchronized SaveOutcome save(RahaJob job, long updatedAt) {
        if (job == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("任务和更新时间必须有效");
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.JOB_RUN)) {
            throw new IllegalStateException("FMDB 任务状态持久化已关闭");
        }
        boolean exists = findByIdempotentKey(job.getDatasetId(),
                job.getIdempotentKey()).isPresent();
        long written = resultWriter.writeJob(tableName, job,
                Collections.<String, Object>emptyMap());
        SaveOutcome outcome = written == 0L ? SaveOutcome.UNCHANGED
                : exists ? SaveOutcome.UPDATED : SaveOutcome.CREATED;
        LOGGER.info("FMDB 任务状态保存完成，jobId={}，status={}，outcome={}",
                job.getJobId(), job.getStatus(), outcome);
        return outcome;
    }

    @Override
    public Optional<RahaJob> findByIdempotentKey(String datasetId,
                                                  String idempotentKey) {
        String validatedDataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String validatedKey = ValueUtils.requireNotBlank(idempotentKey, "幂等键");
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.JOB_RUN)
                || !tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        List<Row> rows = tableGateway.read(tableName,
                        FmdbTableSchemas.columns(FmdbPhysicalTable.JOB_RUN),
                        functions.col("dataset_id").equalTo(validatedDataset)
                                .and(functions.col("idempotent_key")
                                        .equalTo(validatedKey)))
                .orderBy(functions.col("state_version").desc()).limit(1)
                .collectAsList();
        return rows.isEmpty() ? Optional.<RahaJob>empty()
                : Optional.of(toJob(rows.get(0)));
    }

    private static RahaJob toJob(Row row) {
        return RahaJob.restore((String) row.getAs("job_id"),
                (String) row.getAs("idempotent_key"),
                JobType.valueOf((String) row.getAs("job_type")),
                (String) row.getAs("dataset_id"), (String) row.getAs("snapshot_id"),
                (String) row.getAs("config_version"),
                ((Number) row.getAs("created_at")).longValue(),
                JobStatus.valueOf((String) row.getAs("status")),
                (String) row.getAs("current_stage_id"),
                ((Number) row.getAs("started_at")).longValue(),
                ((Number) row.getAs("finished_at")).longValue(),
                (String) row.getAs("error_code"),
                (String) row.getAs("error_message"));
    }
}
