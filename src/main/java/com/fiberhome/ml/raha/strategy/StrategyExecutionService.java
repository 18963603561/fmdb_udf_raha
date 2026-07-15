package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.StrategyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    public StrategyExecutionService(StrategyExecutor executor,
                                    StrategyRepository repository,
                                    Clock clock) {
        if (executor == null || repository == null || clock == null) {
            throw new IllegalArgumentException("策略执行服务依赖不能为空");
        }
        this.executor = executor;
        this.repository = repository;
        this.clock = clock;
    }

    public StrategyBatchResult execute(String jobId,
                                       String stageId,
                                       RahaDataset dataset,
                                       List<StrategyPlan> plans,
                                       long timeoutMillis,
                                       ArtifactVersion version) {
        if (plans == null || version == null) {
            throw new IllegalArgumentException("策略计划集合和结果版本不能为空");
        }
        Set<String> strategyIds = new HashSet<String>();
        List<StrategyExecutionResult> executions = new ArrayList<StrategyExecutionResult>();
        LOGGER.info("开始执行策略批次，jobId={}，planCount={}", jobId, plans.size());
        for (StrategyPlan plan : plans) {
            if (!strategyIds.add(plan.getStrategyId())) {
                throw new IllegalArgumentException("策略批次包含重复标识：" + plan.getStrategyId());
            }
            StrategyExecutionResult result = executor.execute(
                    jobId, stageId, dataset, plan, timeoutMillis);
            repository.saveExecution(result, version, clock.millis());
            executions.add(result);
        }
        StrategyBatchResult batchResult = new StrategyBatchResult(executions);
        LOGGER.info("策略批次执行完成，jobId={}，failedCount={}，hitCount={}",
                jobId, batchResult.getFailedCount(), batchResult.getHits().size());
        return batchResult;
    }
}
