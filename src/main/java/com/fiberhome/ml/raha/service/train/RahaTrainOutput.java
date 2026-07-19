package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationResult;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
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
    /** 训练前确定的模型集合版本。 */
    private final String modelSetVersion;
    /** 可选三类训练物理产物校验结果。 */
    private final TrainingArtifactMaterializationResult materializationResult;

    public RahaTrainOutput(List<StrategyPlan> strategyPlans,
                           StrategyBatchResult strategyBatch,
                           FeatureAssemblyResult features,
                           ClusteringBatchResult clustering,
                           LabelPropagationResult propagation,
                           Map<String, ColumnModelTrainingResult> trainingResults,
                           Map<String, RahaColumnModel> candidateModels,
                           String strategyPlanVersion) {
        this(strategyPlans, strategyBatch, features, clustering, propagation,
                trainingResults, candidateModels, strategyPlanVersion, null, null);
    }

    public RahaTrainOutput(List<StrategyPlan> strategyPlans,
                           StrategyBatchResult strategyBatch,
                           FeatureAssemblyResult features,
                           ClusteringBatchResult clustering,
                           LabelPropagationResult propagation,
                           Map<String, ColumnModelTrainingResult> trainingResults,
                           Map<String, RahaColumnModel> candidateModels,
                           String strategyPlanVersion,
                           String modelSetVersion,
                           TrainingArtifactMaterializationResult materializationResult) {
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
        this.modelSetVersion = modelSetVersion;
        this.materializationResult = materializationResult;
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
    public String getModelSetVersion() { return modelSetVersion; }
    public TrainingArtifactMaterializationResult getMaterializationResult() {
        return materializationResult;
    }
}
