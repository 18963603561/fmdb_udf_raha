package com.fiberhome.ml.raha.job.execution;

import com.fiberhome.ml.raha.config.dto.FailureToleranceConfig;
import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.config.validation.ConfigVersioner;
import com.fiberhome.ml.raha.config.validation.RahaConfigValidator;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.StageStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.id.IdempotencyKeyGenerator;
import com.fiberhome.ml.raha.job.id.RahaIdGenerator;
import com.fiberhome.ml.raha.job.stage.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.StageHandler;
import com.fiberhome.ml.raha.job.stage.StageResult;
import com.fiberhome.ml.raha.repository.adapter.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证任务幂等提交、阶段串联、重试、容忍失败和终止分支。
 */
class RahaJobOrchestratorTest {

    @Test
    void shouldReturnExistingJobForDuplicateSubmission() {
        Fixture fixture = new Fixture(defaultConfig());

        RahaJob first = fixture.orchestrator.submit(fixture.config);
        RahaJob second = fixture.orchestrator.submit(fixture.config);

        assertEquals(first.getJobId(), second.getJobId());
        assertEquals(1, fixture.repository.size());
    }

    @Test
    void shouldChainStagesBindSnapshotAndRetryRecoverableFailure() {
        Fixture fixture = new Fixture(defaultConfig());
        RahaJob job = fixture.orchestrator.submit(fixture.config);
        AtomicInteger strategyAttempts = new AtomicInteger();
        List<StageHandler> handlers = Arrays.asList(
                handler(StageType.LOAD_DATA, context -> StageResult.successWithSnapshot("snapshot-v1")),
                handler(StageType.RUN_STRATEGY, context -> strategyAttempts.incrementAndGet() == 1
                        ? StageResult.failure("PARTIAL_FAILURE", "单个策略失败", true, 1L, 100L)
                        : StageResult.success()),
                handler(StageType.GENERATE_FEATURE, context -> StageResult.success()),
                handler(StageType.PREDICT, context -> StageResult.success())
        );

        JobRunResult result = fixture.orchestrator.execute(job, fixture.config, handlers);

        assertEquals(JobStatus.SUCCEEDED, result.getJob().getStatus());
        assertEquals("snapshot-v1", result.getJob().getSnapshotId());
        assertEquals(5, result.getStages().size());
        assertEquals(2, strategyAttempts.get());
        assertEquals(StageStatus.FAILED, result.getStages().get(1).getStatus());
        assertEquals(StageStatus.SUCCEEDED, result.getStages().get(2).getStatus());
    }

    @Test
    void shouldTerminateJobForNonRecoverableFailure() {
        Fixture fixture = new Fixture(defaultConfig());
        RahaJob job = fixture.orchestrator.submit(fixture.config);

        JobRunResult result = fixture.orchestrator.execute(job, fixture.config,
                Collections.singletonList(handler(StageType.LOAD_DATA,
                        context -> StageResult.failure(
                                "DATA_LOAD_FAILED", "读取失败", false, 0L, 0L))));

        assertEquals(JobStatus.FAILED, result.getJob().getStatus());
        assertEquals("DATA_LOAD_FAILED", result.getJob().getErrorCode());
        assertEquals(1, result.getStages().size());
    }

    @Test
    void shouldContinueAfterToleratedFailureWhenRetryDisabled() {
        RahaJobConfig config = configWithFailureTolerance(new FailureToleranceConfig(false, 0.2d, 0));
        Fixture fixture = new Fixture(config);
        RahaJob job = fixture.orchestrator.submit(config);
        StageHandler nextStage = handler(StageType.GENERATE_FEATURE, context -> {
            context.getAttributes().put("continued", Boolean.TRUE);
            return StageResult.success();
        });

        JobRunResult result = fixture.orchestrator.execute(job, config, Arrays.asList(
                handler(StageType.RUN_STRATEGY, context -> StageResult.failure(
                        "PARTIAL_FAILURE", "部分策略失败", true, 1L, 10L)),
                nextStage));

        assertEquals(JobStatus.PARTIAL_SUCCESS, result.getJob().getStatus());
        assertEquals(StageStatus.FAILED, result.getStages().get(0).getStatus());
        assertSame(Boolean.TRUE, result.getAttributes().get("continued"));
    }

    @Test
    void shouldTerminateWhenLoadedSnapshotConflictsWithSubmittedSnapshot() {
        RahaJobConfig config = new RahaJobConfig(JobType.DETECTION, "dataset", "expected-snapshot",
                "input", "id", false, 1L,
                StrategyConfig.defaults(), FeatureConfig.defaults(), ModelConfig.defaults(),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());
        Fixture fixture = new Fixture(config);
        RahaJob job = fixture.orchestrator.submit(config);

        JobRunResult result = fixture.orchestrator.execute(job, config,
                Collections.singletonList(handler(StageType.LOAD_DATA,
                        context -> StageResult.successWithSnapshot("actual-snapshot"))));

        assertEquals(JobStatus.FAILED, result.getJob().getStatus());
        assertEquals("SNAPSHOT_CONFLICT", result.getJob().getErrorCode());
    }

    @Test
    void shouldRejectDuplicatedStageTypesBeforeStartingJob() {
        Fixture fixture = new Fixture(defaultConfig());
        RahaJob job = fixture.orchestrator.submit(fixture.config);
        StageHandler first = handler(StageType.LOAD_DATA, context -> StageResult.success());
        StageHandler second = handler(StageType.LOAD_DATA, context -> StageResult.success());

        assertThrows(IllegalArgumentException.class,
                () -> fixture.orchestrator.execute(job, fixture.config, Arrays.asList(first, second)));
        assertEquals(JobStatus.CREATED, job.getStatus());
    }

    private static StageHandler handler(StageType stageType, HandlerBody body) {
        return new StageHandler() {
            @Override
            public StageType getStageType() {
                return stageType;
            }

            @Override
            public StageResult execute(StageExecutionContext context) {
                return body.execute(context);
            }
        };
    }

    private static RahaJobConfig defaultConfig() {
        return configWithFailureTolerance(FailureToleranceConfig.defaults());
    }

    private static RahaJobConfig configWithFailureTolerance(FailureToleranceConfig toleranceConfig) {
        return new RahaJobConfig(JobType.DETECTION, "dataset", null,
                "input", "id", false, 1L,
                StrategyConfig.defaults(), FeatureConfig.defaults(), ModelConfig.defaults(),
                ResourceConfig.defaults(), toleranceConfig);
    }

    private interface HandlerBody {
        StageResult execute(StageExecutionContext context);
    }

    private static final class Fixture {

        /** 测试任务配置。 */
        private final RahaJobConfig config;
        /** 测试统一仓储。 */
        private final InMemoryRahaRepository repository;
        /** 测试任务编排器。 */
        private final RahaJobOrchestrator orchestrator;

        private Fixture(RahaJobConfig config) {
            this.config = config;
            this.repository = new InMemoryRahaRepository();
            this.orchestrator = new RahaJobOrchestrator(
                    new RahaConfigValidator(), new ConfigVersioner(),
                    new IdempotencyKeyGenerator(), new SequentialIdGenerator(),
                    new StageFailureDecider(), new DefaultJobRepository(repository),
                    new DefaultStageRepository(repository),
                    Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC));
        }
    }

    private static final class SequentialIdGenerator implements RahaIdGenerator {

        /** 下一个任务序号。 */
        private int nextJobId = 1;

        @Override
        public String newJobId() {
            return "job-" + nextJobId++;
        }

        @Override
        public String newStageId(String jobId, StageType stageType, int attemptId) {
            return jobId + "-" + stageType.name() + "-" + attemptId;
        }
    }
}
