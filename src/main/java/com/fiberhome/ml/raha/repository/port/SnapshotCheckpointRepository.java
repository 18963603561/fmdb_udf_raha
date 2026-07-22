package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 保存和恢复采样快照级前置产物。
 *
 * <p>该仓储面向采样与训练跨任务复用，键由 datasetId、snapshotId 和执行配置指纹共同决定。</p>
 */
public interface SnapshotCheckpointRepository {

    void save(String sourceJobId,
              RahaDataset dataset,
              DatasetSnapshot snapshot,
              List<StrategyPlan> strategyPlans,
              String strategyPlanVersion,
              StrategyBatchResult strategyBatch,
              FeatureAssemblyResult features,
              ClusteringBatchResult clustering,
              String configFingerprint,
              long createdAt);

    default Optional<SnapshotPreparedArtifacts> restore(
            String datasetId,
            String snapshotId,
            String configFingerprint) {
        return restore(datasetId, snapshotId, configFingerprint,
                java.util.Collections.<String>emptySet());
    }

    /**
     * 按字段范围恢复采样快照检查点。
     *
     * <p>字段集合为空时恢复完整检查点；非空时只恢复指定可检测字段，
     * 同时保留行标识等不可检测字段元数据。</p>
     *
     * @param datasetId 数据集标识
     * @param snapshotId 快照标识
     * @param configFingerprint 执行配置指纹
     * @param includedColumns 需要恢复的可检测字段，为空表示全部字段
     * @return 匹配的完整或字段裁剪检查点
     */
    Optional<SnapshotPreparedArtifacts> restore(String datasetId,
                                                String snapshotId,
                                                String configFingerprint,
                                                Set<String> includedColumns);
}
