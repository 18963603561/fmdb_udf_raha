package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StrategyStatus;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.parallel.ParallelBatchResult;
import com.fiberhome.ml.raha.parallel.ParallelWorkItem;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.StrategyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 逐个隔离执行策略并保存完整结果，失败策略不影响其他策略提交。
 */
public final class StrategyExecutionService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(StrategyExecutionService.class);
    /** 单策略执行器。 */
    private final StrategyExecutor executor;
    /** 策略仓储。 */
    private final StrategyRepository repository;
    /** 提供可测试更新时间的时钟。 */
    private final Clock clock;
    /** 受并发和批次超时控制的通用执行器。 */
    private final BoundedParallelExecutor parallelExecutor;
    /** 合并执行同批一对多关系策略的批量执行器。 */
    private final RvdBatchStrategyExecutor rvdBatchExecutor;

    public StrategyExecutionService(StrategyExecutor executor,
                                    StrategyRepository repository,
                                    Clock clock) {
        this(executor, repository, clock, new BoundedParallelExecutor(),
                new RvdBatchStrategyExecutor(clock));
    }

    public StrategyExecutionService(StrategyExecutor executor,
                                    StrategyRepository repository,
                                    Clock clock,
                                    BoundedParallelExecutor parallelExecutor) {
        this(executor, repository, clock, parallelExecutor,
                new RvdBatchStrategyExecutor(clock));
    }

    public StrategyExecutionService(StrategyExecutor executor,
                                    StrategyRepository repository,
                                    Clock clock,
                                    BoundedParallelExecutor parallelExecutor,
                                    RvdBatchStrategyExecutor rvdBatchExecutor) {
        if (executor == null || repository == null || clock == null) {
            throw new IllegalArgumentException("策略执行服务依赖不能为空");
        }
        if (parallelExecutor == null || rvdBatchExecutor == null) {
            throw new IllegalArgumentException("策略并行执行器不能为空");
        }
        this.executor = executor;
        this.repository = repository;
        this.clock = clock;
        this.parallelExecutor = parallelExecutor;
        this.rvdBatchExecutor = rvdBatchExecutor;
    }

    public StrategyBatchResult execute(String jobId,
                                       String stageId,
                                       RahaDataset dataset,
                                       List<StrategyPlan> plans,
                                       long timeoutMillis,
                                       ArtifactVersion version) {
        return execute(jobId, stageId, dataset, plans, timeoutMillis, version,
                1, safeBatchTimeout(timeoutMillis, plans == null ? 1 : plans.size()));
    }

    /**
     * 按配置并发执行策略，单策略仍使用自身超时，整个批次额外受阶段超时约束。
     *
     * @param jobId 任务标识
     * @param stageId 阶段标识
     * @param dataset 只读输入数据集
     * @param plans 策略计划
     * @param strategyTimeoutMillis 单策略超时
     * @param version 仓储业务版本
     * @param maxParallelStrategies 最大策略并发数
     * @param batchTimeoutMillis 策略批次超时
     * @return 按计划顺序汇总的策略结果
     */
    public StrategyBatchResult execute(String jobId,
                                       String stageId,
                                       RahaDataset dataset,
                                       List<StrategyPlan> plans,
                                       long strategyTimeoutMillis,
                                       ArtifactVersion version,
                                       int maxParallelStrategies,
                                       long batchTimeoutMillis) {
        if (plans == null || version == null) {
            throw new IllegalArgumentException("策略计划集合和结果版本不能为空");
        }
        if (maxParallelStrategies <= 0 || batchTimeoutMillis <= 0L) {
            throw new IllegalArgumentException("策略并发数和批次超时必须有效");
        }
        Set<String> strategyIds = new HashSet<String>();
        List<StrategyPlan> rvdPlans = new ArrayList<StrategyPlan>();
        List<StrategyPlan> regularPlans = new ArrayList<StrategyPlan>();
        List<ParallelWorkItem<String, StrategyExecutionResult>> workItems =
                new ArrayList<ParallelWorkItem<String, StrategyExecutionResult>>();
        for (StrategyPlan plan : plans) {
            if (!strategyIds.add(plan.getStrategyId())) {
                throw new IllegalArgumentException("策略批次包含重复标识：" + plan.getStrategyId());
            }
            if (isBatchRvd(plan)) {
                rvdPlans.add(plan);
            } else {
                regularPlans.add(plan);
                workItems.add(new ParallelWorkItem<String, StrategyExecutionResult>(
                        plan.getStrategyId(), () -> executor.execute(jobId, stageId,
                        dataset, plan, strategyTimeoutMillis)));
            }
        }
        LOGGER.info("开始执行策略批次，jobId={}，planCount={}，regularCount={}，rvdBatchCount={}，"
                        + "maxParallelStrategies={}，batchTimeoutMillis={}",
                jobId, plans.size(), regularPlans.size(), rvdPlans.size(),
                maxParallelStrategies, batchTimeoutMillis);
        ParallelBatchResult<String, StrategyExecutionResult> parallelResult =
                parallelExecutor.execute(workItems, maxParallelStrategies, batchTimeoutMillis);
        if (!parallelResult.getFailures().isEmpty()) {
            throw new IllegalStateException("策略并行调度失败："
                    + parallelResult.getFailures().keySet());
        }
        Map<String, StrategyExecutionResult> results =
                new LinkedHashMap<String, StrategyExecutionResult>();
        results.putAll(parallelResult.getSuccesses());
        if (!rvdPlans.isEmpty()) {
            long rvdTimeoutMillis = Math.min(batchTimeoutMillis,
                    safeBatchTimeout(strategyTimeoutMillis, rvdPlans.size()));
            for (StrategyExecutionResult result : rvdBatchExecutor.execute(
                    jobId, stageId, dataset, rvdPlans, rvdTimeoutMillis)) {
                results.put(result.getSummary().getStrategyId(), result);
            }
        }
        List<StrategyExecutionResult> executions = new ArrayList<StrategyExecutionResult>();
        for (StrategyPlan plan : plans) {
            StrategyExecutionResult result = results.get(plan.getStrategyId());
            if (result == null) {
                throw new IllegalStateException("策略批次缺少执行结果：" + plan.getStrategyId());
            }
            repository.saveExecution(result, version, clock.millis());
            executions.add(result);
        }
        StrategyBatchResult batchResult = new StrategyBatchResult(executions);
        LOGGER.info("策略并行批次执行完成，jobId={}，failedCount={}，hitCount={}，"
                        + "maxObservedConcurrency={}",
                jobId, batchResult.getFailedCount(), batchResult.getHits().size(),
                parallelResult.getMaxObservedConcurrency());
        return batchResult;
    }

    private static boolean isBatchRvd(StrategyPlan plan) {
        return plan != null && plan.getStrategyFamily()
                == com.fiberhome.ml.raha.data.StrategyFamily.RVD
                && StrategyTypes.RVD_ONE_TO_MANY.equals(plan.getConfiguration().get(
                StrategyConfigurationKeys.STRATEGY_TYPE));
    }

    /**
     * 复用配置一致的成功策略，只重新执行失败、缺失或配置变化的策略。
     *
     * @param previous 前一次策略批次结果
     * @return 合并成功结果和重跑结果后的完整批次
     */
    public StrategyBatchResult resumeFailed(String jobId,
                                            String stageId,
                                            RahaDataset dataset,
                                            List<StrategyPlan> plans,
                                            StrategyBatchResult previous,
                                            long strategyTimeoutMillis,
                                            ArtifactVersion version,
                                            int maxParallelStrategies,
                                            long batchTimeoutMillis) {
        if (previous == null || plans == null) {
            throw new IllegalArgumentException("恢复策略批次和计划不能为空");
        }
        Map<String, StrategyExecutionResult> reusable =
                new LinkedHashMap<String, StrategyExecutionResult>();
        for (StrategyExecutionResult execution : previous.getExecutions()) {
            if (execution.getSummary().getStatus() == StrategyStatus.SUCCEEDED) {
                reusable.put(execution.getSummary().getStrategyId(), execution);
            }
        }
        List<StrategyPlan> rerunPlans = new ArrayList<StrategyPlan>();
        for (StrategyPlan plan : plans) {
            StrategyExecutionResult existing = reusable.get(plan.getStrategyId());
            if (existing == null || !existing.getSummary().getConfigurationHash()
                    .equals(plan.getConfigurationHash())) {
                rerunPlans.add(plan);
                reusable.remove(plan.getStrategyId());
            }
        }
        StrategyBatchResult rerun = rerunPlans.isEmpty()
                ? new StrategyBatchResult(Collections.<StrategyExecutionResult>emptyList())
                : execute(jobId, stageId, dataset, rerunPlans,
                strategyTimeoutMillis, version, maxParallelStrategies, batchTimeoutMillis);
        Map<String, StrategyExecutionResult> rerunById =
                new LinkedHashMap<String, StrategyExecutionResult>();
        for (StrategyExecutionResult execution : rerun.getExecutions()) {
            rerunById.put(execution.getSummary().getStrategyId(), execution);
        }
        List<StrategyExecutionResult> merged = new ArrayList<StrategyExecutionResult>();
        for (StrategyPlan plan : plans) {
            StrategyExecutionResult execution = reusable.containsKey(plan.getStrategyId())
                    ? reusable.get(plan.getStrategyId()) : rerunById.get(plan.getStrategyId());
            if (execution == null) {
                throw new IllegalStateException("策略恢复后缺少结果：" + plan.getStrategyId());
            }
            merged.add(execution);
        }
        LOGGER.info("失败策略恢复完成，jobId={}，reusedCount={}，rerunCount={}",
                jobId, reusable.size(), rerunPlans.size());
        return new StrategyBatchResult(merged);
    }

    private static long safeBatchTimeout(long timeoutMillis, int itemCount) {
        int count = Math.max(1, itemCount);
        if (timeoutMillis > Long.MAX_VALUE / count) {
            return Long.MAX_VALUE;
        }
        return timeoutMillis * count;
    }
}
