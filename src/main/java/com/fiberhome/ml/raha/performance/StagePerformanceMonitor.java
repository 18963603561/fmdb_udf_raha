package com.fiberhome.ml.raha.performance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.Callable;

/**
 * 统一测量检测阶段耗时和 Driver JVM 堆内存变化，并记录成功或失败日志。
 */
public final class StagePerformanceMonitor {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            StagePerformanceMonitor.class);
    /** 提供可测试指标时间的时钟。 */
    private final Clock clock;
    /** 阶段资源采集探针。 */
    private final StageResourceProbe resourceProbe;

    public StagePerformanceMonitor(Clock clock) {
        this(clock, new JvmStageResourceProbe(clock));
    }

    public StagePerformanceMonitor(Clock clock,
                                   StageResourceProbe resourceProbe) {
        if (clock == null || resourceProbe == null) {
            throw new IllegalArgumentException("阶段性能监控依赖不能为空");
        }
        this.clock = clock;
        this.resourceProbe = resourceProbe;
    }

    /**
     * 执行阶段任务并返回业务结果与性能指标，异常会记录完整堆栈后继续抛出。
     */
    public <T> MeasuredStageResult<T> measure(String stageName,
                                              StageExecutionContext context,
                                              Callable<T> task) {
        if (stageName == null || stageName.trim().isEmpty()
                || context == null || task == null) {
            throw new IllegalArgumentException("阶段性能测量参数不能为空");
        }
        StageResourceSnapshot resourceBefore = resourceProbe.capture();
        long startedAtNanos = System.nanoTime();
        LOGGER.info("开始采集阶段性能基线，stageName={}，inputRowCount={}，"
                        + "dataColumnCount={}，strategyCount={}，partitionCount={}，cacheEnabled={}",
                stageName, context.getInputRowCount(), context.getDataColumnCount(),
                context.getStrategyCount(), context.getPartitionCount(),
                context.isCacheEnabled());
        try {
            T result = task.call();
            StagePerformanceMetric metric = metric(stageName, context,
                    startedAtNanos, resourceBefore, resourceProbe.capture(), true);
            LOGGER.info("阶段性能基线采集完成，stageName={}，elapsedMillis={}，"
                            + "driverHeapDeltaBytes={}，executorMemoryDeltaBytes={}，"
                            + "diskDeltaBytes={}，networkDeltaBytes={}",
                    stageName, metric.getElapsedMillis(), metric.getHeapDeltaBytes(),
                    metric.getExecutorMemoryDeltaBytes(), metric.getDiskDeltaBytes(),
                    metric.getNetworkDeltaBytes());
            return new MeasuredStageResult<T>(result, metric);
        } catch (RuntimeException exception) {
            LOGGER.error("阶段性能基线任务执行失败，stageName={}", stageName, exception);
            throw exception;
        } catch (Exception exception) {
            LOGGER.error("阶段性能基线任务执行失败，stageName={}", stageName, exception);
            throw new IllegalStateException("阶段性能基线任务执行失败：" + stageName,
                    exception);
        }
    }

    private StagePerformanceMetric metric(String stageName,
                                          StageExecutionContext context,
                                          long startedAtNanos,
                                          StageResourceSnapshot resourceBefore,
                                          StageResourceSnapshot resourceAfter,
                                          boolean succeeded) {
        long elapsedNanos = Math.max(0L, System.nanoTime() - startedAtNanos);
        long elapsedMillis = elapsedNanos / 1000000L;
        return new StagePerformanceMetric(stageName, context, elapsedMillis,
                resourceBefore, resourceAfter, succeeded,
                Math.max(1L, clock.millis()));
    }
}
