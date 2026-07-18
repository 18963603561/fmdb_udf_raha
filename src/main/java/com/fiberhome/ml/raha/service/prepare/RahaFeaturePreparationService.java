package com.fiberhome.ml.raha.service.prepare;

import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanVersioner;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统一生成并缓存可由主动采样和模型训练复用的策略及特征产物。
 */
public final class RahaFeaturePreparationService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RahaFeaturePreparationService.class);
    /** 策略计划服务。 */
    private final StrategyPlanService planService;
    /** 策略执行服务。 */
    private final StrategyExecutionService executionService;
    /** 特征组装服务。 */
    private final FeatureService featureService;
    /** 提供可测试任务时间的时钟。 */
    private final Clock clock;

    public RahaFeaturePreparationService(StrategyPlanService planService,
                                         StrategyExecutionService executionService,
                                         FeatureService featureService,
                                         Clock clock) {
        if (planService == null || executionService == null
                || featureService == null || clock == null) {
            throw new IllegalArgumentException("特征准备服务依赖不能为空");
        }
        this.planService = planService;
        this.executionService = executionService;
        this.featureService = featureService;
        this.clock = clock;
    }

    /**
     * 执行策略计划、批量策略和特征组装，阶段结束后释放自行创建的输入缓存。
     *
     * @param request 特征准备请求
     * @return 可供采样和训练共同复用的不可变产物
     */
    public RahaFeaturePreparationResult prepare(RahaFeaturePreparationRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("特征准备请求不能为空");
        }
        long startNanos = System.nanoTime();
        Dataset<Row> inputFrame = request.getDataset().getDataFrame();
        boolean ownsInputCache = inputFrame.storageLevel().equals(StorageLevel.NONE());
        LOGGER.info("开始准备 Raha 策略和特征，jobId={}，datasetId={}，snapshotId={}",
                request.getJobId(), request.getDataset().getDatasetId(),
                request.getDataset().getSnapshotId());
        try {
            if (ownsInputCache) {
                StorageLevel storageLevel = StorageLevel.fromString(
                        request.getConfig().getResourceConfig().getCacheStorageLevel());
                LOGGER.info("开始缓存特征准备公共输入，jobId={}，storageLevel={}",
                        request.getJobId(), storageLevel.description());
                inputFrame.persist(storageLevel);
                inputFrame.count();
            }
            List<StrategyPlan> plans = planService.generateAndSave(
                    request.getDataset(), request.getConfig().getStrategyConfig(),
                    request.getArtifactVersion());
            StrategyBatchResult strategyBatch = executionService.execute(
                    request.getJobId(), request.getStageId() + "-strategy",
                    request.getDataset(), plans,
                    request.getConfig().getStrategyConfig().getStrategyTimeoutMillis(),
                    request.getArtifactVersion(),
                    request.getConfig().getResourceConfig().getMaxParallelStrategies(),
                    request.getConfig().getResourceConfig().getStageTimeoutMillis());
            FeatureAssemblyResult features = featureService.assembleAndSave(
                    request.getJobId(), request.getDataset(), plans,
                    strategyBatch.getHits(), request.getConfig().getFeatureConfig(),
                    request.getArtifactVersion());
            String planVersion = StrategyPlanVersioner.versionOf(plans);
            long runtimeMillis = Math.max(0L, TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - startNanos));
            LOGGER.info("Raha 策略和特征准备完成，jobId={}，planCount={}，hitCount={}，"
                            + "featureRowCount={}，runtimeMillis={}",
                    request.getJobId(), plans.size(), strategyBatch.getHitCount(),
                    features.getRows().size(), runtimeMillis);
            return new RahaFeaturePreparationResult(
                    request.getDataset().getDatasetId(),
                    request.getDataset().getSnapshotId(), plans, strategyBatch,
                    features, planVersion, runtimeMillis);
        } catch (RuntimeException | LinkageError exception) {
            LOGGER.error("Raha 策略和特征准备失败，jobId={}，datasetId={}",
                    request.getJobId(), request.getDataset().getDatasetId(), exception);
            throw exception;
        } finally {
            if (ownsInputCache) {
                inputFrame.unpersist(false);
                LOGGER.info("特征准备公共输入缓存已释放，jobId={}", request.getJobId());
            }
        }
    }

}
