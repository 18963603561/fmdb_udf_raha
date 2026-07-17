package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.fmdb.FmdbResultWriter;
import com.fiberhome.ml.raha.job.RahaIdGenerator;
import com.fiberhome.ml.raha.job.RahaJob;
import com.fiberhome.ml.raha.repository.JobRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Optional;

/**
 * 使用任务仓储和 FMDB 任务表提交异步任务，并执行显式幂等冲突检查。
 */
public final class RepositoryBackedRahaUdfJobSubmitter implements RahaUdfJobSubmitter {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RepositoryBackedRahaUdfJobSubmitter.class);
    /** 核心任务仓储。 */
    private final JobRepository jobRepository;
    /** FMDB 任务状态写入器。 */
    private final FmdbResultWriter resultWriter;
    /** FMDB 任务状态表。 */
    private final String jobTable;
    /** 任务标识生成器。 */
    private final RahaIdGenerator idGenerator;
    /** 提供可测试提交时间的时钟。 */
    private final Clock clock;
    public RepositoryBackedRahaUdfJobSubmitter(JobRepository jobRepository,
                                               FmdbResultWriter resultWriter,
                                               String jobTable,
                                               RahaIdGenerator idGenerator,
                                               Clock clock) {
        if (jobRepository == null || resultWriter == null
                || idGenerator == null || clock == null) {
            throw new IllegalArgumentException("UDF 异步任务提交器依赖不能为空");
        }
        this.jobRepository = jobRepository;
        this.resultWriter = resultWriter;
        this.jobTable = com.fiberhome.ml.raha.fmdb.SparkSqlFmdbTableGateway
                .validateTableName(jobTable);
        this.idGenerator = idGenerator;
        this.clock = clock;
    }

    @Override
    public synchronized RahaUdfSubmissionResult submit(RahaUdfRequest request) {
        if (request == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT", "UDF 请求不能为空");
        }
        return submitRequest(request, Math.max(1L, clock.millis()));
    }

    private RahaUdfSubmissionResult submitRequest(RahaUdfRequest request,
                                                  long submittedAt) {
        String configVersion = HashUtils.sha256Hex(request.toCanonicalConfiguration());
        Optional<RahaJob> existing = jobRepository.findByIdempotentKey(
                request.getDatasetId(), request.getIdempotencyKey());
        if (existing.isPresent()) {
            RahaJob job = existing.get();
            // 相同幂等键不能指向另一套输入或运行参数。
            if (!job.getConfigVersion().equals(configVersion)
                    || job.getJobType() != request.toJobType()) {
                throw new RahaUdfException("IDEMPOTENCY_CONFLICT",
                        "相同幂等键已用于其他任务配置");
            }
            resultWriter.writeJob(jobTable, job);
            LOGGER.info("UDF 重复提交返回已有任务，jobId={}，taskType={}，caller={}",
                    job.getJobId(), request.getTaskType(), request.getCaller());
            return RahaUdfSubmissionResult.duplicate(job.getJobId(),
                    request.getTaskType(), resultLocation(request, job.getJobId()),
                    configVersion, submittedAt);
        }
        RahaJob job = new RahaJob(idGenerator.newJobId(), request.getIdempotencyKey(),
                request.toJobType(), request.getDatasetId(), request.getSnapshotId(),
                configVersion, submittedAt);
        jobRepository.save(job, submittedAt);
        // 任务先进入核心仓储，再幂等写 FMDB；外部写入失败后相同请求可安全补写。
        resultWriter.writeJob(jobTable, job);
        LOGGER.info("UDF 异步任务提交完成，jobId={}，taskType={}，caller={}，status=CREATED",
                job.getJobId(), request.getTaskType(), request.getCaller());
        return RahaUdfSubmissionResult.accepted(
                job.getJobId(), request.getTaskType(),
                resultLocation(request, job.getJobId()), configVersion, submittedAt);
    }

    private static String resultLocation(RahaUdfRequest request, String jobId) {
        return "fmdb://" + request.getResultTable() + "/" + jobId;
    }
}
