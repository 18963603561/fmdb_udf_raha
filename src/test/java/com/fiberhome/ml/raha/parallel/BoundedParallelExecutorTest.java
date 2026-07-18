package com.fiberhome.ml.raha.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证通用并行执行器的限流、顺序、异常隔离、超时和重复工作键保护。
 */
class BoundedParallelExecutorTest {

    @Test
    void shouldLimitConcurrencyAndKeepInputOrder() {
        AtomicInteger active = new AtomicInteger();
        AtomicInteger observedPeak = new AtomicInteger();
        List<ParallelWorkItem<Integer, String>> items =
                new ArrayList<ParallelWorkItem<Integer, String>>();
        for (int index = 0; index < 6; index++) {
            final int value = index;
            items.add(new ParallelWorkItem<Integer, String>(index, () -> {
                int current = active.incrementAndGet();
                observedPeak.updateAndGet(previous -> Math.max(previous, current));
                try {
                    Thread.sleep(60L);
                    return "value-" + value;
                } finally {
                    active.decrementAndGet();
                }
            }));
        }

        ParallelBatchResult<Integer, String> result =
                new BoundedParallelExecutor().execute(items, 2, 3000L);

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5),
                new ArrayList<Integer>(result.getSuccesses().keySet()));
        assertEquals(0, result.getFailures().size());
        assertEquals(2, result.getMaxObservedConcurrency());
        assertEquals(2, observedPeak.get());
    }

    @Test
    void shouldIsolateItemFailureAndEnforceBatchTimeout() {
        List<ParallelWorkItem<String, String>> failureItems = Arrays.asList(
                new ParallelWorkItem<String, String>("success", () -> "ok"),
                new ParallelWorkItem<String, String>("failure", () -> {
                    throw new IllegalArgumentException("测试失败");
                }));

        ParallelBatchResult<String, String> failureResult =
                new BoundedParallelExecutor().execute(failureItems, 2, 1000L);

        assertEquals("ok", failureResult.getSuccesses().get("success"));
        assertEquals("IllegalArgumentException",
                failureResult.getFailures().get("failure").getErrorType());
        assertFalse(failureResult.getFailures().get("failure").isTimeout());

        List<ParallelWorkItem<String, String>> timeoutItems = Arrays.asList(
                new ParallelWorkItem<String, String>("slow", () -> {
                    Thread.sleep(1000L);
                    return "late";
                }),
                new ParallelWorkItem<String, String>("queued", () -> "quick"));
        ParallelBatchResult<String, String> timeoutResult =
                new BoundedParallelExecutor().execute(timeoutItems, 1, 50L);

        assertTrue(timeoutResult.getFailures().get("slow").isTimeout());
        assertEquals(2, timeoutResult.getSuccesses().size()
                + timeoutResult.getFailures().size());
        // 取消慢任务后排队任务可能抢先完成，两种结果都必须被如实归档。
        if (timeoutResult.getSuccesses().containsKey("queued")) {
            assertEquals("quick", timeoutResult.getSuccesses().get("queued"));
        } else {
            assertTrue(timeoutResult.getFailures().get("queued").isTimeout());
        }

        List<ParallelWorkItem<String, String>> partialItems = Arrays.asList(
                new ParallelWorkItem<String, String>("slow", () -> {
                    Thread.sleep(1000L);
                    return "late";
                }),
                new ParallelWorkItem<String, String>("completed", () -> "available"));
        ParallelBatchResult<String, String> partialResult =
                new BoundedParallelExecutor().execute(partialItems, 2, 50L);

        assertTrue(partialResult.getFailures().get("slow").isTimeout());
        assertEquals("available", partialResult.getSuccesses().get("completed"));
    }

    @Test
    void shouldRejectDuplicateWorkKeys() {
        List<ParallelWorkItem<String, String>> items = Arrays.asList(
                new ParallelWorkItem<String, String>("same", () -> "first"),
                new ParallelWorkItem<String, String>("same", () -> "second"));

        assertThrows(IllegalArgumentException.class,
                () -> new BoundedParallelExecutor().execute(items, 2, 1000L));
    }
}
