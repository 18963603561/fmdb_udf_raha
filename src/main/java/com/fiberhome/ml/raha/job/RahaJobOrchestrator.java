package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.config.ConfigVersioner;
import com.fiberhome.ml.raha.config.RahaConfigValidator;
import com.fiberhome.ml.raha.config.RahaJobConfig;
import com.fiberhome.ml.raha.data.JobStatus;
import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.observability.RahaLogContext;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.JobRepository;
import com.fiberhome.ml.raha.repository.StageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 创建幂等任务并按处理器顺序执行阶段、重试、继续或终止分支。
 */
public final class RahaJobOrchestrator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaJobOrchestrator.class);
    /** 任务配置校验器。 */
    private final RahaConfigValidator configValidator;
    /** 配置版本生成器。 */
    private final ConfigVersioner configVersioner;
    /** 任务幂等键生成器。 */
    private final IdempotencyKeyGenerator idempotencyKeyGenerator;
    /** 任务和阶段标识生成器。 */
    private final RahaIdGenerator idGenerator;
    /** 阶段失败决策器。 */
    private final StageFailureDecider failureDecider;
    /** 任务仓储。 */
    private final JobRepository jobRepository;
    /** 阶段仓储。 */
    private final StageRepository stageRepository;
    /** 提供可测试时间的时钟。 */
    private final Clock clock;

    public RahaJobOrchestrator(RahaConfigValidator configValidator,
                               ConfigVersioner configVersioner,
                               IdempotencyKeyGenerator idempotencyKeyGenerator,
                               RahaIdGenerator idGenerator,
                               StageFailureDecider failureDecider,
                               JobRepository jobRepository,
                               StageRepository stageRepository,
                               Clock clock) {
        if (configValidator == null || configVersioner == null
                || idempotencyKeyGenerator == null || idGenerator == null
                || failureDecider == null || jobRepository == null
                || stageRepository == null || clock == null) {
            throw new IllegalArgumentException("任务编排器依赖不能为空");
        }
        this.configValidator = configValidator;
        this.configVersioner = configVersioner;
        this.idempotencyKeyGenerator = idempotencyKeyGenerator;
        this.idGenerator = idGenerator;
        this.failureDecider = failureDecider;
        this.jobRepository = jobRepository;
        this.stageRepository = stageRepository;
        this.clock = clock;
    }

    /**
     * 提交幂等任务，相同配置重复提交时返回已有任务。
     *
     * @param config 任务配置
     * @return 新建或已有任务
     */
    public synchronized RahaJob submit(RahaJobConfig config) {
        configValidator.validate(config);
        String configVersion = configVersioner.versionOf(config);
        String idempotentKey = idempotencyKeyGenerator.generate(config, configVersion);
        Optional<RahaJob> existing = jobRepository.findByIdempotentKey(
                config.getDatasetId(), idempotentKey);
        if (existing.isPresent()) {
            LOGGER.info("检测到重复任务提交，返回已有任务，jobId={}", existing.get().getJobId());
            return existing.get();
        }
        RahaJob job = new RahaJob(idGenerator.newJobId(), idempotentKey,
                config.getJobType(), config.getDatasetId(), config.getSnapshotId(),
                configVersion, clock.millis());
        jobRepository.save(job, clock.millis());
        LOGGER.info("Raha 任务创建完成，jobId={}，jobType={}", job.getJobId(), job.getJobType());
        return job.snapshot();
    }

    /**
     * 按给定顺序执行任务阶段。
     *
     * @param submittedJob 已提交任务
     * @param config 任务配置
     * @param handlers 有序阶段处理器
     * @return 任务运行结果
     */
    public JobRunResult execute(RahaJob submittedJob,
                                RahaJobConfig config,
                                List<StageHandler> handlers) {
        if (submittedJob == null || handlers == null || handlers.isEmpty()) {
            throw new IllegalArgumentException("已提交任务和阶段处理器不能为空");
        }
        configValidator.validate(config);
        String configVersion = configVersioner.versionOf(config);
        if (!submittedJob.getConfigVersion().equals(configVersion)) {
            throw new IllegalArgumentException("任务配置版本与已提交任务不一致");
        }
        if (submittedJob.getStatus() != JobStatus.CREATED) {
            throw new IllegalStateException("只有已创建任务可以开始执行");
        }

        RahaJob job = submittedJob.snapshot();
        List<RahaStage> stages = new ArrayList<RahaStage>();
        Map<String, Object> attributes = new LinkedHashMap<String, Object>();
        boolean firstStage = true;
        Set<StageType> stageTypes = new HashSet<StageType>();
        for (StageHandler handler : handlers) {
            if (handler == null || handler.getStageType() == null) {
                throw new IllegalArgumentException("阶段处理器和阶段类型不能为空");
            }
            if (!stageTypes.add(handler.getStageType())) {
                throw new IllegalArgumentException("同一任务流水线不能重复定义阶段类型：" + handler.getStageType());
            }
        }
        long executionStartedAt = clock.millis();
        LOGGER.info("Raha 任务开始执行，jobId={}，configVersion={}，snapshotId={}，stageCount={}",
                job.getJobId(), job.getConfigVersion(), job.getSnapshotId(), handlers.size());

        for (StageHandler handler : handlers) {
            int attemptId = 1;
            while (true) {
                String stageId = idGenerator.newStageId(job.getJobId(), handler.getStageType(), attemptId);
                if (firstStage) {
                    job.start(stageId, clock.millis());
                    firstStage = false;
                } else {
                    job.moveToStage(stageId);
                }
                jobRepository.save(job, clock.millis());

                RahaStage stage = new RahaStage(stageId, job.getJobId(), handler.getStageType(), attemptId);
                stage.start(clock.millis());
                saveStage(stage, job);
                RahaLogContext logContext = new RahaLogContext(job.getJobId(), stageId,
                        attemptId, job.getSnapshotId());
                LOGGER.info("Raha 阶段开始，context={}，stageType={}",
                        logContext.toLogText(), handler.getStageType());
                StageResult result = executeHandler(handler, job, config, stage, attributes);
                if (result.getSnapshotId() != null) {
                    try {
                        job.bindSnapshot(result.getSnapshotId());
                    } catch (RuntimeException exception) {
                        // 任务配置快照与加载结果冲突时禁止继续使用不确定输入。
                        LOGGER.error("任务绑定输入快照失败，jobId={}，snapshotId={}",
                                job.getJobId(), result.getSnapshotId(), exception);
                        result = StageResult.failure("SNAPSHOT_CONFLICT",
                                exception.getMessage(), false, 0L, 0L);
                    }
                }

                if (result.getOutcome() == StageOutcome.SUCCESS) {
                    stage.succeed(clock.millis());
                    saveStage(stage, job);
                    stages.add(stage.snapshot());
                    jobRepository.save(job, clock.millis());
                    RahaLogContext successLogContext = new RahaLogContext(job.getJobId(), stageId,
                            attemptId, job.getSnapshotId());
                    LOGGER.info("Raha 阶段成功，context={}，stageType={}，elapsedMillis={}，"
                                    + "totalItemCount={}，failedItemCount={}",
                            successLogContext.toLogText(), handler.getStageType(),
                            stage.getFinishedAt() - stage.getStartedAt(),
                            result.getTotalItemCount(), result.getFailedItemCount());
                    break;
                }
                if (result.getOutcome() == StageOutcome.SKIPPED) {
                    stage.skip(clock.millis());
                    saveStage(stage, job);
                    stages.add(stage.snapshot());
                    LOGGER.info("Raha 阶段跳过，context={}，stageType={}，elapsedMillis={}",
                            logContext.toLogText(), handler.getStageType(),
                            stage.getFinishedAt() - stage.getStartedAt());
                    break;
                }

                stage.fail(result.getErrorCode(), safeMessage(result.getMessage()), clock.millis());
                saveStage(stage, job);
                stages.add(stage.snapshot());
                LOGGER.warn("Raha 阶段失败，context={}，stageType={}，errorCode={}，"
                                + "elapsedMillis={}，totalItemCount={}，failedItemCount={}",
                        logContext.toLogText(), handler.getStageType(), result.getErrorCode(),
                        stage.getFinishedAt() - stage.getStartedAt(),
                        result.getTotalItemCount(), result.getFailedItemCount());
                FailureDecision decision = failureDecider.decide(
                        result, config.getFailureToleranceConfig(), attemptId);
                if (decision == FailureDecision.RETRY) {
                    LOGGER.warn("阶段执行失败，准备重试，jobId={}，stageType={}，attemptId={}",
                            job.getJobId(), handler.getStageType(), attemptId);
                    attemptId++;
                    continue;
                }
                if (decision == FailureDecision.CONTINUE) {
                    LOGGER.warn("阶段存在可容忍失败，继续后续阶段，jobId={}，stageType={}",
                            job.getJobId(), handler.getStageType());
                    break;
                }
                long executionFinishedAt = clock.millis();
                job.fail(result.getErrorCode(), safeMessage(result.getMessage()), executionFinishedAt);
                jobRepository.save(job, clock.millis());
                LOGGER.error("Raha 任务因阶段失败终止，jobId={}，stageType={}，errorCode={}，"
                                + "elapsedMillis={}，stageAttemptCount={}",
                        job.getJobId(), handler.getStageType(), result.getErrorCode(),
                        executionFinishedAt - executionStartedAt, stages.size());
                return new JobRunResult(job, stages, attributes);
            }
        }

        long executionFinishedAt = clock.millis();
        job.succeed(executionFinishedAt);
        jobRepository.save(job, clock.millis());
        LOGGER.info("Raha 任务执行成功，jobId={}，stageAttemptCount={}，elapsedMillis={}",
                job.getJobId(), stages.size(), executionFinishedAt - executionStartedAt);
        return new JobRunResult(job, stages, attributes);
    }

    private StageResult executeHandler(StageHandler handler,
                                       RahaJob job,
                                       RahaJobConfig config,
                                       RahaStage stage,
                                       Map<String, Object> attributes) {
        try {
            StageResult result = handler.execute(new StageExecutionContext(
                    job.snapshot(), config, stage.snapshot(), attributes));
            if (result == null) {
                return StageResult.failure("STAGE_RESULT_REQUIRED",
                        "阶段处理器返回空结果", false, 0L, 0L);
            }
            return result;
        } catch (RuntimeException exception) {
            // 未处理异常视为不可恢复阶段失败，并记录完整堆栈和任务上下文。
            LOGGER.error("阶段处理器执行异常，jobId={}，stageType={}，attemptId={}",
                    job.getJobId(), stage.getStageType(), stage.getAttemptId(), exception);
            return StageResult.failure("STAGE_EXECUTION_ERROR",
                    exception.getMessage(), false, 0L, 0L);
        }
    }

    private void saveStage(RahaStage stage, RahaJob job) {
        String snapshotId = job.getSnapshotId() == null ? "PENDING_SNAPSHOT" : job.getSnapshotId();
        ArtifactVersion version = new ArtifactVersion(
                job.getConfigVersion(), snapshotId, stage.getStageId(), stage.getAttemptId());
        stageRepository.save(stage, version, clock.millis());
    }

    private static String safeMessage(String message) {
        return message == null || message.trim().isEmpty() ? "阶段执行失败" : message;
    }
}
