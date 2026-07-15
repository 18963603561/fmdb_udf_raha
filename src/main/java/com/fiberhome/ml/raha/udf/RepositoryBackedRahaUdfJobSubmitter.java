package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.audit.RahaAuditAction;
import com.fiberhome.ml.raha.audit.RahaAuditService;
import com.fiberhome.ml.raha.audit.RahaAuditStatus;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.fmdb.FmdbResultWriter;
import com.fiberhome.ml.raha.job.RahaIdGenerator;
import com.fiberhome.ml.raha.job.RahaJob;
import com.fiberhome.ml.raha.repository.JobRepository;
import com.fiberhome.ml.raha.security.AllowAllRahaPermissionChecker;
import com.fiberhome.ml.raha.security.RahaAccessController;
import com.fiberhome.ml.raha.security.RahaAccessDeniedException;
import com.fiberhome.ml.raha.security.RahaPermissionAction;
import com.fiberhome.ml.raha.security.RahaPermissionChecker;
import com.fiberhome.ml.raha.security.RahaPermissionRequest;
import com.fiberhome.ml.raha.security.RahaResourceType;
import com.fiberhome.ml.raha.service.RahaTaskType;
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
    /** FMDB 任务审计表。 */
    private final String jobTable;
    /** 任务标识生成器。 */
    private final RahaIdGenerator idGenerator;
    /** 提供可测试提交时间的时钟。 */
    private final Clock clock;
    /** 生产资源访问控制器。 */
    private final RahaAccessController accessController;
    /** 任务提交审计服务。 */
    private final RahaAuditService auditService;

    public RepositoryBackedRahaUdfJobSubmitter(JobRepository jobRepository,
                                               FmdbResultWriter resultWriter,
                                               String jobTable,
                                               RahaIdGenerator idGenerator,
                                               Clock clock) {
        this(jobRepository, resultWriter, jobTable, idGenerator, clock,
                AllowAllRahaPermissionChecker.getInstance(),
                RahaAuditService.noOp(clock));
    }

    public RepositoryBackedRahaUdfJobSubmitter(JobRepository jobRepository,
                                               FmdbResultWriter resultWriter,
                                               String jobTable,
                                               RahaIdGenerator idGenerator,
                                               Clock clock,
                                               RahaPermissionChecker permissionChecker,
                                               RahaAuditService auditService) {
        if (jobRepository == null || resultWriter == null
                || idGenerator == null || clock == null
                || permissionChecker == null || auditService == null) {
            throw new IllegalArgumentException("UDF 异步任务提交器依赖不能为空");
        }
        this.jobRepository = jobRepository;
        this.resultWriter = resultWriter;
        this.jobTable = com.fiberhome.ml.raha.fmdb.SparkSqlFmdbTableGateway
                .validateTableName(jobTable);
        this.idGenerator = idGenerator;
        this.clock = clock;
        this.accessController = new RahaAccessController(permissionChecker);
        this.auditService = auditService;
    }

    @Override
    public synchronized RahaUdfSubmissionResult submit(RahaUdfRequest request) {
        if (request == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT", "UDF 请求不能为空");
        }
        long submittedAt = Math.max(1L, clock.millis());
        try {
            authorize(request);
        } catch (RahaAccessDeniedException exception) {
            auditService.record(request.getCaller(), RahaAuditAction.TASK_SUBMIT,
                    RahaAuditStatus.DENIED, RahaResourceType.TASK,
                    request.getTaskType().name(), request.getDatasetId(),
                    null, request.getModelVersion(), "任务提交权限校验拒绝");
            throw new RahaUdfException("PERMISSION_DENIED",
                    "调用方没有提交或访问任务资源的权限", exception);
        }
        try {
            return submitAuthorized(request, submittedAt);
        } catch (RuntimeException exception) {
            // 业务提交失败仍写入不含原始值的失败审计，审计失败不能覆盖原始异常。
            try {
                auditService.record(request.getCaller(), RahaAuditAction.TASK_SUBMIT,
                        RahaAuditStatus.FAILED, RahaResourceType.TASK,
                        request.getTaskType().name(), request.getDatasetId(),
                        null, request.getModelVersion(), "任务提交执行失败");
            } catch (RuntimeException auditException) {
                LOGGER.error("任务提交失败审计写入异常，datasetId={}，taskType={}",
                        request.getDatasetId(), request.getTaskType(), auditException);
            }
            throw exception;
        }
    }

    private RahaUdfSubmissionResult submitAuthorized(RahaUdfRequest request,
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
            RahaUdfSubmissionResult result = RahaUdfSubmissionResult.duplicate(job.getJobId(),
                    request.getTaskType(), resultLocation(request, job.getJobId()),
                    configVersion, submittedAt);
            auditTaskSubmit(request, job.getJobId(), "任务重复提交并返回已有任务");
            return result;
        }
        RahaJob job = new RahaJob(idGenerator.newJobId(), request.getIdempotencyKey(),
                request.toJobType(), request.getDatasetId(), request.getSnapshotId(),
                configVersion, submittedAt);
        jobRepository.save(job, submittedAt);
        // 任务先进入核心仓储，再幂等写 FMDB；外部写入失败后相同请求可安全补写。
        resultWriter.writeJob(jobTable, job);
        LOGGER.info("UDF 异步任务提交完成，jobId={}，taskType={}，caller={}，status=CREATED",
                job.getJobId(), request.getTaskType(), request.getCaller());
        RahaUdfSubmissionResult result = RahaUdfSubmissionResult.accepted(
                job.getJobId(), request.getTaskType(),
                resultLocation(request, job.getJobId()), configVersion, submittedAt);
        auditTaskSubmit(request, job.getJobId(), "异步任务提交成功");
        return result;
    }

    private void authorize(RahaUdfRequest request) {
        require(request, RahaPermissionAction.SUBMIT, RahaResourceType.TASK,
                request.getTaskType().name());
        String inputResource = request.getSourceType() == DataFormat.FMDB_TABLE
                ? request.getInputReference()
                : "fmdb-query:" + request.getDatasetId();
        require(request, RahaPermissionAction.READ, RahaResourceType.INPUT_DATA,
                inputResource);
        require(request, RahaPermissionAction.WRITE, RahaResourceType.RESULT_DATA,
                request.getResultTable());
        // 训练需要读取标注表，检测需要读取明确模型版本。
        if (request.getTaskType() == RahaTaskType.TRAIN) {
            require(request, RahaPermissionAction.READ,
                    RahaResourceType.ANNOTATION_DATA,
                    request.getAnnotationReference());
        } else if (request.getTaskType() == RahaTaskType.DETECT) {
            require(request, RahaPermissionAction.READ, RahaResourceType.MODEL,
                    request.getModelVersion());
        }
    }

    private void require(RahaUdfRequest request,
                         RahaPermissionAction action,
                         RahaResourceType resourceType,
                         String resourceName) {
        accessController.requireAllowed(new RahaPermissionRequest(
                request.getCaller(), action, resourceType, resourceName,
                request.getDatasetId()));
    }

    private void auditTaskSubmit(RahaUdfRequest request,
                                 String jobId,
                                 String summary) {
        auditService.record(request.getCaller(), RahaAuditAction.TASK_SUBMIT,
                RahaAuditStatus.SUCCEEDED, RahaResourceType.TASK,
                request.getTaskType().name(), request.getDatasetId(), jobId,
                request.getModelVersion(), summary);
    }

    private static String resultLocation(RahaUdfRequest request, String jobId) {
        return "fmdb://" + request.getResultTable() + "/" + jobId;
    }
}
