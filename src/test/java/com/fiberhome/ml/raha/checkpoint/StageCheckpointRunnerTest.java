package com.fiberhome.ml.raha.checkpoint;

import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultStageCheckpointRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.StageCheckpointRepository;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证阶段检查点的重试审计、严格版本复用、输入变化重算和幂等恢复。
 */
class StageCheckpointRunnerTest {

    @Test
    void shouldRetryRecoverableFailureAndReuseSameInputWithoutDuplicateResult() {
        StageCheckpointRepository repository = repository();
        StageCheckpointRunner runner = runner(repository);
        AtomicInteger invocations = new AtomicInteger();
        ArtifactVersion version = version("snapshot-v1");

        CheckpointRunResult<String> first = runner.run("job-1", StageType.TRAIN,
                version, "fingerprint-v1", 2, attemptId -> {
                    if (invocations.incrementAndGet() == 1) {
                        return CheckpointTaskResult.failure("TEMPORARY_FAILURE",
                                "临时训练失败", true,
                                Collections.singletonMap("attempt", String.valueOf(attemptId)));
                    }
                    return CheckpointTaskResult.success("trained-model",
                            "model://job-1/column-code",
                            Collections.singletonMap("modelCount", "1"));
                });

        assertTrue(first.isSucceeded());
        assertFalse(first.isReused());
        assertEquals("trained-model", first.getPayload());
        assertEquals(2, first.getExecutedAttempts());
        assertEquals(2, invocations.get());
        List<StageCheckpoint> attempts = repository.findAttempts("job-1", StageType.TRAIN);
        assertEquals(2, attempts.size());
        assertEquals(StageCheckpointStatus.FAILED, attempts.get(0).getStatus());
        assertEquals(StageCheckpointStatus.SUCCEEDED, attempts.get(1).getStatus());

        CheckpointRunResult<String> reused = runner.run("job-1", StageType.TRAIN,
                version, "fingerprint-v1", 2, attemptId -> {
                    invocations.incrementAndGet();
                    return CheckpointTaskResult.success("duplicate",
                            "model://duplicate", Collections.<String, String>emptyMap());
                });

        assertTrue(reused.isSucceeded());
        assertTrue(reused.isReused());
        assertNull(reused.getPayload());
        assertEquals("model://job-1/column-code", reused.getOutputLocation());
        assertEquals(0, reused.getExecutedAttempts());
        assertEquals(2, invocations.get());
        assertEquals(2, repository.findAttempts("job-1", StageType.TRAIN).size());
    }

    @Test
    void shouldRecomputeWhenFingerprintOrSnapshotChanges() {
        StageCheckpointRepository repository = repository();
        StageCheckpointRunner runner = runner(repository);
        AtomicInteger invocations = new AtomicInteger();
        CheckpointTask<String> task = attemptId -> CheckpointTaskResult.success(
                "result-" + invocations.incrementAndGet(), "output://" + attemptId,
                Collections.<String, String>emptyMap());

        CheckpointRunResult<String> original = runner.run("job-2", StageType.PREDICT,
                version("snapshot-v1"), "fingerprint-v1", 0, task);
        CheckpointRunResult<String> fingerprintChanged = runner.run(
                "job-2", StageType.PREDICT, version("snapshot-v1"),
                "fingerprint-v2", 0, task);
        CheckpointRunResult<String> snapshotChanged = runner.run(
                "job-2", StageType.PREDICT, version("snapshot-v2"),
                "fingerprint-v2", 0, task);

        assertFalse(original.isReused());
        assertFalse(fingerprintChanged.isReused());
        assertFalse(snapshotChanged.isReused());
        assertEquals(3, invocations.get());
        assertEquals(3, repository.findAttempts("job-2", StageType.PREDICT).size());
        assertEquals(3, snapshotChanged.getCheckpoint().getAttemptId());
    }

    @Test
    void shouldAuditEveryFailedAttemptAndNotRetryUnexpectedException() {
        StageCheckpointRepository repository = repository();
        StageCheckpointRunner runner = runner(repository);

        CheckpointRunResult<String> failed = runner.run("job-3", StageType.CLUSTER,
                version("snapshot-v1"), "failure-input", 2,
                attemptId -> CheckpointTaskResult.failure("RETRYABLE",
                        "聚类资源暂不可用", true,
                        Collections.singletonMap("attempt", String.valueOf(attemptId))));

        assertFalse(failed.isSucceeded());
        assertEquals(3, failed.getExecutedAttempts());
        assertEquals(3, repository.findAttempts("job-3", StageType.CLUSTER).size());

        AtomicInteger exceptionInvocations = new AtomicInteger();
        CheckpointRunResult<String> exceptionResult = runner.run(
                "job-4", StageType.GENERATE_FEATURE, version("snapshot-v1"),
                "exception-input", 3, attemptId -> {
                    exceptionInvocations.incrementAndGet();
                    throw new IllegalStateException("确定性代码错误");
                });

        assertFalse(exceptionResult.isSucceeded());
        assertEquals("UNEXPECTED_STAGE_EXCEPTION", exceptionResult.getErrorCode());
        assertEquals(1, exceptionInvocations.get());
        assertEquals(1, repository.findAttempts(
                "job-4", StageType.GENERATE_FEATURE).size());
    }

    private static StageCheckpointRunner runner(StageCheckpointRepository repository) {
        return new StageCheckpointRunner(repository,
                Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC));
    }

    private static StageCheckpointRepository repository() {
        return new DefaultStageCheckpointRepository(new InMemoryRahaRepository());
    }

    private static ArtifactVersion version(String snapshotId) {
        return new ArtifactVersion("config-v1", snapshotId, "input-stage", 1);
    }
}
