package com.fiberhome.ml.raha.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 以固定线程数执行工作项，限制并发并对整个批次应用统一超时。
 */
public final class BoundedParallelExecutor {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            BoundedParallelExecutor.class);

    /**
     * 按输入顺序返回结果，单项异常被隔离，批次超时后取消尚未完成工作项。
     *
     * @param items 有稳定唯一键的工作项
     * @param maxConcurrency 最大并发数
     * @param timeoutMillis 整个批次超时时间
     * @param <K> 工作项键类型
     * @param <V> 工作项结果类型
     * @return 成功、失败和实际并发峰值
     */
    public <K, V> ParallelBatchResult<K, V> execute(
            List<ParallelWorkItem<K, V>> items,
            int maxConcurrency,
            long timeoutMillis) {
        if (items == null || maxConcurrency <= 0 || timeoutMillis <= 0L) {
            throw new IllegalArgumentException("并行工作项、并发数和超时必须有效");
        }
        Set<K> keys = new LinkedHashSet<K>();
        for (ParallelWorkItem<K, V> item : items) {
            if (item == null || !keys.add(item.getKey())) {
                throw new IllegalArgumentException("并行工作项不能包含空值或重复键");
            }
        }
        if (items.isEmpty()) {
            return new ParallelBatchResult<K, V>(
                    new LinkedHashMap<K, V>(),
                    new LinkedHashMap<K, ParallelFailure>(), 0);
        }
        int threadCount = Math.min(maxConcurrency, items.size());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount, runnable -> {
            Thread thread = new Thread(runnable, "raha-bounded-worker");
            thread.setDaemon(true);
            return thread;
        });
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Map<K, Future<V>> futures = new LinkedHashMap<K, Future<V>>();
        long startedAt = System.nanoTime();
        long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long deadline = timeoutNanos >= Long.MAX_VALUE - startedAt
                ? Long.MAX_VALUE : startedAt + timeoutNanos;
        try {
            for (ParallelWorkItem<K, V> item : items) {
                futures.put(item.getKey(), executor.submit(() -> {
                    int current = active.incrementAndGet();
                    updatePeak(peak, current);
                    try {
                        return item.getCallable().call();
                    } finally {
                        active.decrementAndGet();
                    }
                }));
            }
            Map<K, V> successes = new LinkedHashMap<K, V>();
            Map<K, ParallelFailure> failures =
                    new LinkedHashMap<K, ParallelFailure>();
            boolean timedOut = false;
            for (Map.Entry<K, Future<V>> entry : futures.entrySet()) {
                if (timedOut) {
                    // 批次截止后保留已经完成的工作结果，只取消仍在运行或排队的任务。
                    if (entry.getValue().isDone()) {
                        collectCompleted(entry, successes, failures);
                    } else {
                        entry.getValue().cancel(true);
                        failures.put(entry.getKey(), timeoutFailure());
                    }
                    continue;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) {
                    timedOut = true;
                    if (entry.getValue().isDone()) {
                        collectCompleted(entry, successes, failures);
                    } else {
                        entry.getValue().cancel(true);
                        failures.put(entry.getKey(), timeoutFailure());
                    }
                    continue;
                }
                try {
                    successes.put(entry.getKey(), entry.getValue().get(
                            remaining, TimeUnit.NANOSECONDS));
                } catch (TimeoutException exception) {
                    timedOut = true;
                    entry.getValue().cancel(true);
                    failures.put(entry.getKey(), timeoutFailure());
                    LOGGER.warn("并行批次执行超时，workKey={}，timeoutMillis={}",
                            entry.getKey(), timeoutMillis);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    timedOut = true;
                    entry.getValue().cancel(true);
                    failures.put(entry.getKey(), new ParallelFailure(
                            "InterruptedException", "并行批次等待线程被中断", false));
                } catch (ExecutionException exception) {
                    Throwable cause = exception.getCause() == null
                            ? exception : exception.getCause();
                    failures.put(entry.getKey(), failure(cause));
                    LOGGER.error("并行工作项执行失败，workKey={}", entry.getKey(), cause);
                } catch (CancellationException exception) {
                    failures.put(entry.getKey(), new ParallelFailure(
                            "CancellationException", "并行工作项已取消", timedOut));
                }
            }
            return new ParallelBatchResult<K, V>(successes, failures, peak.get());
        } finally {
            for (Future<V> future : futures.values()) {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            }
            executor.shutdownNow();
        }
    }

    private static void updatePeak(AtomicInteger peak, int current) {
        int observed;
        do {
            observed = peak.get();
            if (current <= observed) {
                return;
            }
        } while (!peak.compareAndSet(observed, current));
    }

    private static <K, V> void collectCompleted(
            Map.Entry<K, Future<V>> entry,
            Map<K, V> successes,
            Map<K, ParallelFailure> failures) {
        try {
            successes.put(entry.getKey(), entry.getValue().get());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            failures.put(entry.getKey(), new ParallelFailure(
                    "InterruptedException", "并行批次等待线程被中断", false));
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause() == null
                    ? exception : exception.getCause();
            failures.put(entry.getKey(), failure(cause));
            LOGGER.error("并行工作项执行失败，workKey={}", entry.getKey(), cause);
        } catch (CancellationException exception) {
            failures.put(entry.getKey(), new ParallelFailure(
                    "CancellationException", "并行工作项已取消", true));
        }
    }

    private static ParallelFailure timeoutFailure() {
        return new ParallelFailure("TimeoutException", "并行批次超过配置超时", true);
    }

    private static ParallelFailure failure(Throwable throwable) {
        String type = throwable == null ? "RuntimeException"
                : throwable.getClass().getSimpleName();
        String message = throwable == null ? null : throwable.getMessage();
        return new ParallelFailure(type == null || type.trim().isEmpty()
                ? "RuntimeException" : type,
                message == null || message.trim().isEmpty()
                        ? "并行工作项执行失败" : message, false);
    }
}
