package com.fiberhome.ml.raha.checkpoint;

import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存可从采样快照检查点恢复的前置产物集合。
 *
 * <p>该对象只承载训练继续执行所需的元数据和中间产物，恢复自 FMDB 时
 * {@link RahaDataset#getDataFrame()} 可以为空，后续训练必须只依赖已恢复的特征和聚类结果。</p>
 */
public final class SnapshotPreparedArtifacts {

    /** 检查点唯一标识。 */
    private final String checkpointId;
    /** 采样阶段的数据集元数据。 */
    private final RahaDataset dataset;
    /** 采样阶段的数据快照元数据。 */
    private final DatasetSnapshot snapshot;
    /** 稳定排序后的策略计划。 */
    private final List<StrategyPlan> strategyPlans;
    /** 只保留摘要的策略执行结果。 */
    private final StrategyBatchResult strategyBatch;
    /** 已生成的特征字典和稀疏特征。 */
    private final FeatureAssemblyResult features;
    /** 策略计划版本。 */
    private final String strategyPlanVersion;
    /** 已完成的列内聚类结果。 */
    private final ClusteringBatchResult clustering;
    /** 参与复用匹配的执行配置指纹。 */
    private final String configFingerprint;

    public SnapshotPreparedArtifacts(String checkpointId,
                                     RahaDataset dataset,
                                     DatasetSnapshot snapshot,
                                     List<StrategyPlan> strategyPlans,
                                     StrategyBatchResult strategyBatch,
                                     FeatureAssemblyResult features,
                                     String strategyPlanVersion,
                                     ClusteringBatchResult clustering,
                                     String configFingerprint) {
        this.checkpointId = ValueUtils.requireNotBlank(
                checkpointId, "快照检查点标识");
        if (dataset == null || snapshot == null || strategyPlans == null
                || strategyBatch == null || features == null || clustering == null) {
            throw new IllegalArgumentException("快照检查点恢复产物不能为空");
        }
        if (!dataset.getDatasetId().equals(snapshot.getDatasetId())
                || !dataset.getSnapshotId().equals(snapshot.getSnapshotId())) {
            throw new IllegalArgumentException("检查点数据集与快照不一致");
        }
        this.dataset = dataset;
        this.snapshot = snapshot;
        this.strategyPlans = Collections.unmodifiableList(
                new ArrayList<StrategyPlan>(strategyPlans));
        this.strategyBatch = strategyBatch;
        this.features = features;
        this.strategyPlanVersion = ValueUtils.requireNotBlank(
                strategyPlanVersion, "策略计划版本");
        this.clustering = clustering;
        this.configFingerprint = ValueUtils.requireNotBlank(
                configFingerprint, "执行配置指纹");
    }

    public String getCheckpointId() { return checkpointId; }
    public RahaDataset getDataset() { return dataset; }
    public DatasetSnapshot getSnapshot() { return snapshot; }
    public List<StrategyPlan> getStrategyPlans() { return strategyPlans; }
    public StrategyBatchResult getStrategyBatch() { return strategyBatch; }
    public FeatureAssemblyResult getFeatures() { return features; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public ClusteringBatchResult getClustering() { return clustering; }
    public String getConfigFingerprint() { return configFingerprint; }
}
