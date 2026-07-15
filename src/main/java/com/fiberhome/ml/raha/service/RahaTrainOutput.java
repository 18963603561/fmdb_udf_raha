package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.cluster.ClusteringBatchResult;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.label.LabelPropagationResult;
import com.fiberhome.ml.raha.model.ColumnModelTrainingResult;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.strategy.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.StrategyPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存训练服务各阶段结果和最终候选列级模型。
 */
public final class RahaTrainOutput {

    /** 确定性策略计划。 */
    private final List<StrategyPlan> strategyPlans;
    /** 策略执行批次。 */
    private final StrategyBatchResult strategyBatch;
    /** 单元格特征和字典。 */
    private final FeatureAssemblyResult features;
    /** 列内聚类结果。 */
    private final ClusteringBatchResult clustering;
    /** 直接标签和传播标签结果。 */
    private final LabelPropagationResult propagation;
    /** 按字段索引的训练结果。 */
    private final Map<String, ColumnModelTrainingResult> trainingResults;
    /** 按字段索引的候选模型元数据。 */
    private final Map<String, RahaColumnModel> candidateModels;
    /** 训练依赖的策略计划版本。 */
    private final String strategyPlanVersion;

    public RahaTrainOutput(List<StrategyPlan> strategyPlans,
                           StrategyBatchResult strategyBatch,
                           FeatureAssemblyResult features,
                           ClusteringBatchResult clustering,
                           LabelPropagationResult propagation,
                           Map<String, ColumnModelTrainingResult> trainingResults,
                           Map<String, RahaColumnModel> candidateModels,
                           String strategyPlanVersion) {
        if (strategyPlans == null || strategyBatch == null || features == null
                || clustering == null || propagation == null || trainingResults == null
                || candidateModels == null || strategyPlanVersion == null) {
            throw new IllegalArgumentException("训练服务输出阶段结果不能为空");
        }
        this.strategyPlans = Collections.unmodifiableList(
                new ArrayList<StrategyPlan>(strategyPlans));
        this.strategyBatch = strategyBatch;
        this.features = features;
        this.clustering = clustering;
        this.propagation = propagation;
        this.trainingResults = Collections.unmodifiableMap(
                new LinkedHashMap<String, ColumnModelTrainingResult>(trainingResults));
        this.candidateModels = Collections.unmodifiableMap(
                new LinkedHashMap<String, RahaColumnModel>(candidateModels));
        this.strategyPlanVersion = strategyPlanVersion;
    }

    public List<StrategyPlan> getStrategyPlans() { return strategyPlans; }
    public StrategyBatchResult getStrategyBatch() { return strategyBatch; }
    public FeatureAssemblyResult getFeatures() { return features; }
    public ClusteringBatchResult getClustering() { return clustering; }
    public LabelPropagationResult getPropagation() { return propagation; }
    public Map<String, ColumnModelTrainingResult> getTrainingResults() {
        return trainingResults;
    }
    public Map<String, RahaColumnModel> getCandidateModels() { return candidateModels; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
}
