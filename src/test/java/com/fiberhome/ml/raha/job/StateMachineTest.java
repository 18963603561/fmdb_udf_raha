package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.data.JobStatus;
import com.fiberhome.ml.raha.data.JobType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证独立异步任务状态转换以及失败时的原子性。
 */
class StateMachineTest {

    @Test
    void shouldCompleteJobLifecycle() {
        RahaJob job = new RahaJob(
                "job-1", "key-1", JobType.DETECTION, "dataset", "snapshot", "config-v1", 100L);

        job.start(110L);
        job.succeed(200L);

        assertEquals(JobStatus.SUCCEEDED, job.getStatus());
        assertEquals(200L, job.getFinishedAt());
        assertNull(job.getErrorCode());
        assertThrows(IllegalStateException.class,
                () -> job.fail("LATE_FAILURE", "不允许覆盖成功状态", 210L));
    }

    @Test
    void shouldKeepJobStateWhenStartValidationFails() {
        RahaJob job = new RahaJob(
                "job-1", "key-1", JobType.DETECTION, "dataset", null, "config-v1", 100L);

        assertThrows(IllegalArgumentException.class, () -> job.start(99L));

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
}
