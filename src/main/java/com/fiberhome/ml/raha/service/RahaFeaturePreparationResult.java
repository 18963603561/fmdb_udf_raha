package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.strategy.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存可由采样和训练共同复用的策略及特征产物。
 */
public final class RahaFeaturePreparationResult {

    /** 数据集标识。 */
    private final String datasetId;
    /** 输入快照标识。 */
    private final String snapshotId;
    /** 确定性策略计划。 */
    private final List<StrategyPlan> strategyPlans;
    /** 策略执行结果。 */
    private final StrategyBatchResult strategyBatch;
    /** 单元格特征和字典。 */
    private final FeatureAssemblyResult features;
    /** 策略计划稳定版本。 */
    private final String strategyPlanVersion;
    /** 特征准备总耗时。 */
    private final long runtimeMillis;

    public RahaFeaturePreparationResult(String datasetId,
                                        String snapshotId,
                                        List<StrategyPlan> strategyPlans,
                                        StrategyBatchResult strategyBatch,
                                        FeatureAssemblyResult features,
                                        String strategyPlanVersion,
                                        long runtimeMillis) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.snapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        this.strategyPlanVersion = ValueUtils.requireNotBlank(
                strategyPlanVersion, "策略计划版本");
        if (strategyPlans == null || strategyBatch == null || features == null
                || runtimeMillis < 0L) {
            throw new IllegalArgumentException("特征准备结果不能为空且耗时不能为负数");
        }
        this.strategyPlans = Collections.unmodifiableList(
                new ArrayList<StrategyPlan>(strategyPlans));
        this.strategyBatch = strategyBatch;
        this.features = features;
        this.runtimeMillis = runtimeMillis;
    }

    public String getDatasetId() { return datasetId; }
    public String getSnapshotId() { return snapshotId; }
    public List<StrategyPlan> getStrategyPlans() { return strategyPlans; }
    public StrategyBatchResult getStrategyBatch() { return strategyBatch; }
    public FeatureAssemblyResult getFeatures() { return features; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public long getRuntimeMillis() { return runtimeMillis; }
}
