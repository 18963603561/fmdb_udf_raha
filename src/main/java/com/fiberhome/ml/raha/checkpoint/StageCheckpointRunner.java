package com.fiberhome.ml.raha.checkpoint;

import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.StageCheckpointRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 统一执行阶段任务，记录每次尝试，并复用输入版本完全一致的成功检查点。
 */
public final class StageCheckpointRunner {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(StageCheckpointRunner.class);
    /** 阶段检查点仓储。 */
    private final StageCheckpointRepository repository;
    /** 提供可测试的审计时间。 */
    private final Clock clock;

    public StageCheckpointRunner(StageCheckpointRepository repository, Clock clock) {
        if (repository == null || clock == null) {
            throw new IllegalArgumentException("阶段检查点执行器依赖不能为空");
        }
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 执行阶段任务；存在可复用检查点时不调用任务，失败时按可恢复标记决定是否重试。
     *
     * @param jobId 任务标识
     * @param stageType 阶段类型
     * @param inputVersion 配置和数据快照版本
     * @param inputFingerprint 上游依赖及输入内容指纹
     * @param maxRetryCount 首次执行之外允许的最大重试次数
     * @param task 阶段业务任务
     * @param <T> 成功结果载荷类型
     * @return 成功、复用或最终失败结果
     */
    public <T> CheckpointRunResult<T> run(String jobId,
                                          StageType stageType,
                                          ArtifactVersion inputVersion,
                                          String inputFingerprint,
                                          int maxRetryCount,
                                          CheckpointTask<T> task) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "检查点任务标识");
        String fingerprint = ValueUtils.requireNotBlank(
                inputFingerprint, "检查点输入指纹");
        if (stageType == null || inputVersion == null || task == null || maxRetryCount < 0) {
            throw new IllegalArgumentException("检查点阶段、版本、任务和重试次数必须有效");
        }
        // 只有配置、快照和输入指纹完全一致时才跳过本次阶段计算。
        Optional<StageCheckpoint> reusable = repository.findReusable(
                validatedJobId, stageType, inputVersion, fingerprint);
        if (reusable.isPresent()) {
            StageCheckpoint checkpoint = reusable.get();
            LOGGER.info("复用阶段成功检查点，jobId={}，stageType={}，attemptId={}，outputLocation={}",
                    validatedJobId, stageType, checkpoint.getAttemptId(),
                    checkpoint.getOutputLocation());
            return CheckpointRunResult.reused(checkpoint);
        }

        List<StageCheckpoint> historical = repository.findAttempts(
                validatedJobId, stageType);
        int nextAttemptId = nextAttemptId(historical);
        int executedAttempts = 0;
        StageCheckpoint terminal = null;
        LOGGER.info("开始执行检查点阶段，jobId={}，stageType={}，maxRetryCount={}",
                validatedJobId, stageType, maxRetryCount);
        for (int retryIndex = 0; retryIndex <= maxRetryCount; retryIndex++) {
            int attemptId = nextAttemptId + retryIndex;
            long startedAt = positiveNow();
            StageCheckpoint running = StageCheckpoint.running(validatedJobId,
                    stageType, attemptId, inputVersion, fingerprint, startedAt);
            // 运行中记录先落库，进程异常退出后仍可审计未完成尝试。
            repository.save(running, startedAt);
            executedAttempts++;
            CheckpointTaskResult<T> taskResult;
            try {
                taskResult = task.execute(attemptId);
                if (taskResult == null) {
                    throw new IllegalStateException("阶段任务返回结果不能为空");
                }
            } catch (RuntimeException exception) {
                // 未声明可恢复性的代码异常按不可重试失败处理，避免重复放大副作用。
                long completedAt = completedAt(startedAt);
                terminal = running.fail("UNEXPECTED_STAGE_EXCEPTION",
                        safeMessage(exception), Collections.<String, String>emptyMap(),
                        completedAt);
                repository.save(terminal, completedAt);
                LOGGER.error("阶段任务异常终止，jobId={}，stageType={}，attemptId={}",
                        validatedJobId, stageType, attemptId, exception);
                return CheckpointRunResult.failed(terminal, executedAttempts);
            }
            long completedAt = completedAt(startedAt);
            if (taskResult.isSucceeded()) {
                terminal = running.succeed(taskResult.getOutputLocation(),
                        taskResult.getSummary(), completedAt);
                repository.save(terminal, completedAt);
                LOGGER.info("检查点阶段执行成功，jobId={}，stageType={}，attemptId={}，"
                                + "executedAttempts={}",
                        validatedJobId, stageType, attemptId, executedAttempts);
                return CheckpointRunResult.succeeded(
                        taskResult.getPayload(), terminal, executedAttempts);
            }
            terminal = running.fail(taskResult.getErrorCode(),
                    taskResult.getErrorMessage(), taskResult.getSummary(), completedAt);
            repository.save(terminal, completedAt);
            boolean retry = taskResult.isRecoverable() && retryIndex < maxRetryCount;
            if (!retry) {
                LOGGER.warn("检查点阶段最终失败，jobId={}，stageType={}，attemptId={}，"
                                + "errorCode={}，recoverable={}",
                        validatedJobId, stageType, attemptId,
                        taskResult.getErrorCode(), taskResult.isRecoverable());
                return CheckpointRunResult.failed(terminal, executedAttempts);
            }
            LOGGER.warn("检查点阶段失败后重试，jobId={}，stageType={}，attemptId={}，"
                            + "nextAttemptId={}，errorCode={}",
                    validatedJobId, stageType, attemptId, attemptId + 1,
                    taskResult.getErrorCode());
        }
        throw new IllegalStateException("检查点重试流程未生成终态结果");
    }

    private static int nextAttemptId(List<StageCheckpoint> historical) {
        int maximum = 0;
        for (StageCheckpoint checkpoint : historical) {
            maximum = Math.max(maximum, checkpoint.getAttemptId());
        }
        return maximum + 1;
    }

    private long positiveNow() {
        return Math.max(1L, clock.millis());
    }

    private long completedAt(long startedAt) {
        return Math.max(startedAt, positiveNow());
    }

    private static String safeMessage(RuntimeException exception) {
        String message = exception.getMessage();
        return message == null || message.trim().isEmpty()
                ? exception.getClass().getSimpleName() : message;
    }
}
