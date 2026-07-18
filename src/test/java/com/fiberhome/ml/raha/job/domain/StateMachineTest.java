package com.fiberhome.ml.raha.job.domain;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.StageStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证任务和阶段状态转换以及失败时的原子性。
 */
class StateMachineTest {

    @Test
    void shouldCompleteJobLifecycle() {
        RahaJob job = new RahaJob(
                "job-1", "key-1", JobType.DETECTION, "dataset", "snapshot", "config-v1", 100L);

        job.start("stage-1", 110L);
        job.moveToStage("stage-2");
        job.succeed(200L);

        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        assertEquals("stage-2", job.getCurrentStageId());
        assertEquals(200L, job.getFinishedAt());
        assertNull(job.getErrorCode());
        assertThrows(IllegalStateException.class,
                () -> job.fail("LATE_FAILURE", "不允许覆盖成功状态", 210L));
    }

    @Test
    void shouldKeepJobStateWhenStartValidationFails() {
        RahaJob job = new RahaJob(
                "job-1", "key-1", JobType.DETECTION, "dataset", null, "config-v1", 100L);

        assertThrows(IllegalArgumentException.class, () -> job.start("stage-1", 99L));

        assertEquals(JobStatus.CREATED, job.getStatus());
        assertEquals(0L, job.getStartedAt());
    }

    @Test
    void shouldRecordJobFailureContext() {
        RahaJob job = new RahaJob(
                "job-1", "key-1", JobType.TRAINING, "dataset", null, "config-v1", 100L);

        job.fail("CONFIG_ERROR", "配置校验失败", 101L);

        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals("CONFIG_ERROR", job.getErrorCode());
        assertEquals("配置校验失败", job.getErrorMessage());
    }

    @Test
    void shouldCompleteStageLifecycle() {
        RahaStage stage = new RahaStage("stage-1", "job-1", StageType.INITIALIZE, 1);

        stage.start(100L);
        stage.succeed(120L);

        assertEquals(StageStatus.SUCCEEDED, stage.getStatus());
        assertEquals(120L, stage.getFinishedAt());
        assertThrows(IllegalStateException.class, () -> stage.cancel(130L));
    }

    @Test
    void shouldKeepStageStateWhenStartValidationFails() {
        RahaStage stage = new RahaStage("stage-1", "job-1", StageType.LOAD_DATA, 1);

        assertThrows(IllegalArgumentException.class, () -> stage.start(0L));

        assertEquals(StageStatus.PENDING, stage.getStatus());
        assertEquals(0L, stage.getStartedAt());
    }
}

