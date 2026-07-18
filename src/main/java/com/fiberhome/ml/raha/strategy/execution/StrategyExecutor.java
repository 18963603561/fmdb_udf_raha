package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.strategy.api.DetectionStrategy;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.plan.StrategyCandidate;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 执行单个策略并隔离超时和异常，确保失败策略不会写入部分命中。
 */
public final class StrategyExecutor {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(StrategyExecutor.class);
    /** 策略实现注册表。 */
    private final StrategyRegistry registry;
    /** 提供可测试完成时间的时钟。 */
    private final Clock clock;

    public StrategyExecutor(StrategyRegistry registry, Clock clock) {
        if (registry == null || clock == null) {
            throw new IllegalArgumentException("策略注册表和时钟不能为空");
        }
        this.registry = registry;
        this.clock = clock;
    }

    /**
     * 执行一个策略，超时或异常均转换为失败摘要。
     *
     * @param jobId 任务标识
     * @param stageId 阶段标识
     * @param dataset 只读数据集
     * @param plan 策略计划
     * @param timeoutMillis 超时时间
     * @return 策略执行结果
     */
    public StrategyExecutionResult execute(String jobId,
                                            String stageId,
                                            RahaDataset dataset,
                                            StrategyPlan plan,
                                            long timeoutMillis) {
        ValueUtils.requireNotBlank(jobId, "任务标识");
        ValueUtils.requireNotBlank(stageId, "阶段标识");
        if (dataset == null || plan == null || timeoutMillis <= 0L) {
            throw new IllegalArgumentException("策略执行参数非法");
        }
        DetectionStrategy strategy = registry.get(
                plan.getConfiguration().get(StrategyConfigurationKeys.STRATEGY_TYPE));
        if (strategy.getStrategyFamily() != plan.getStrategyFamily()) {
            throw new IllegalArgumentException("策略实现和计划的策略族不一致");
        }
        LOGGER.info("策略开始执行，jobId={}，strategyId={}，strategyType={}，timeoutMillis={}",
                jobId, plan.getStrategyId(), strategy.getStrategyType(), timeoutMillis);
        long startNanos = System.nanoTime();
        long inputCount = inputCount(dataset, plan);
        String jobGroup = jobId + ":" + plan.getStrategyId();
        ExecutorService worker = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "raha-strategy-" + plan.getStrategyId());
            thread.setDaemon(true);
            return thread;
        });
        Future<List<StrategyCandidate>> future = worker.submit(new Callable<List<StrategyCandidate>>() {
            @Override
            public List<StrategyCandidate> call() {
                SparkSession sparkSession = dataset.getDataFrame().sparkSession();
                sparkSession.sparkContext().setJobGroup(jobGroup,
                        "Raha strategy " + plan.getStrategyId(), true);
                try {
                    List<StrategyCandidate> candidates = strategy.detect(
                            new StrategyExecutionContext(jobId, stageId, dataset, plan));
                    return candidates == null
                            ? Collections.<StrategyCandidate>emptyList() : candidates;
                } finally {
                    sparkSession.sparkContext().clearJobGroup();
                }
            }
        });
        try {
            List<StrategyCandidate> candidates = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            long runtime = elapsedMillis(startNanos);
            List<StrategyHit> hits = toHits(jobId, stageId, dataset, plan, candidates, runtime);
            StrategyRunSummary summary = summary(jobId, stageId, dataset, plan,
                    StrategyStatus.SUCCEEDED, inputCount, hits.size(), runtime, null, null);
            LOGGER.info("策略执行完成，jobId={}，strategyId={}，hitCount={}，runtimeMillis={}",
                    jobId, plan.getStrategyId(), hits.size(), runtime);
            return new StrategyExecutionResult(summary, hits);
        } catch (TimeoutException exception) {
            future.cancel(true);
            cancelSparkJob(dataset, jobGroup);
            long runtime = elapsedMillis(startNanos);
            LOGGER.warn("策略执行超时，jobId={}，strategyId={}，timeoutMillis={}",
                    jobId, plan.getStrategyId(), timeoutMillis);
            return failed(jobId, stageId, dataset, plan, inputCount, runtime,
                    "STRATEGY_TIMEOUT", "策略执行超过超时时间");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            cancelSparkJob(dataset, jobGroup);
            long runtime = elapsedMillis(startNanos);
            LOGGER.error("策略执行线程被中断，jobId={}，strategyId={}",
                    jobId, plan.getStrategyId(), exception);
            return failed(jobId, stageId, dataset, plan, inputCount, runtime,
                    "STRATEGY_INTERRUPTED", "策略执行线程被中断");
        } catch (ExecutionException exception) {
            long runtime = elapsedMillis(startNanos);
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            LOGGER.error("策略执行失败，jobId={}，strategyId={}",
                    jobId, plan.getStrategyId(), cause);
            return failed(jobId, stageId, dataset, plan, inputCount, runtime,
                    "STRATEGY_EXECUTION_FAILED", safeMessage(cause));
        } finally {
            worker.shutdownNow();
        }
    }

    private StrategyExecutionResult failed(String jobId,
                                           String stageId,
                                           RahaDataset dataset,
                                           StrategyPlan plan,
                                           long inputCount,
                                           long runtime,
                                           String errorCode,
                                           String errorMessage) {
        return new StrategyExecutionResult(summary(jobId, stageId, dataset, plan,
                StrategyStatus.FAILED, inputCount, 0L, runtime, errorCode, errorMessage),
                Collections.<StrategyHit>emptyList());
    }

    private StrategyRunSummary summary(String jobId,
                                       String stageId,
                                       RahaDataset dataset,
                                       StrategyPlan plan,
                                       StrategyStatus status,
                                       long inputCount,
                                       long hitCount,
                                       long runtime,
                                       String errorCode,
                                       String errorMessage) {
        return new StrategyRunSummary(jobId, stageId, dataset.getSnapshotId(),
                plan.getStrategyId(), plan.getConfigurationHash(), plan.getStrategyFamily(),
                status, inputCount, hitCount, runtime, errorCode, errorMessage, clock.millis());
    }

    private static List<StrategyHit> toHits(String jobId,
                                            String stageId,
                                            RahaDataset dataset,
                                            StrategyPlan plan,
                                            List<StrategyCandidate> candidates,
                                            long runtime) {
        List<StrategyHit> hits = new ArrayList<StrategyHit>(candidates.size());
        for (StrategyCandidate candidate : candidates) {
            if (!plan.getTargetColumns().contains(candidate.getColumnName())) {
                throw new IllegalArgumentException("策略候选字段不属于计划目标字段");
            }
            CellCoordinate coordinate = new CellCoordinate(dataset.getDatasetId(),
                    dataset.getSnapshotId(), candidate.getRowId(), candidate.getColumnName());
            hits.add(new StrategyHit(jobId, stageId, plan.getStrategyId(),
                    plan.getStrategyFamily(), coordinate, candidate.getValueHash(),
                    candidate.getReasonCode(), candidate.getReasonDetails(), candidate.getScore(),
                    runtime, StrategyStatus.SUCCEEDED));
        }
        return hits;
    }

    private static long inputCount(RahaDataset dataset, StrategyPlan plan) {
        ColumnProfile profile = dataset.getProfiles().get(plan.getTargetColumns().get(0));
        if (profile != null) {
            return profile.getTotalCount();
        }
        return 0L;
    }

    private static void cancelSparkJob(RahaDataset dataset, String jobGroup) {
        try {
            if (dataset.getDataFrame() != null) {
                dataset.getDataFrame().sparkSession().sparkContext().cancelJobGroup(jobGroup);
            }
        } catch (RuntimeException exception) {
            LOGGER.warn("取消超时 Spark 作业失败，jobGroup={}", jobGroup, exception);
        }
    }

    private static long elapsedMillis(long startNanos) {
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
    }

    private static String safeMessage(Throwable throwable) {
        String message = throwable == null ? null : throwable.getMessage();
        return message == null || message.trim().isEmpty() ? "策略执行失败" : message;
    }
}
