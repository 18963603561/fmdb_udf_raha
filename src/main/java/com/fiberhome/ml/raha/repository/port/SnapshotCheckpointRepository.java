package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
import com.fiberhome.ml.raha.checkpoint.SnapshotCheckpointWriteSession;
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

    /**
     * 返回采样前置处理建议使用的逻辑列批大小。
     */
    default int getColumnBatchSize() {
        return 10;
    }

    /**
     * 创建分批写会话，但不提交最终清单。
     */
    SnapshotCheckpointWriteSession begin(
            String sourceJobId,
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            List<StrategyPlan> strategyPlans,
            String strategyPlanVersion,
            String configFingerprint,
            long createdAt);

    /**
     * 原子保存一个已完成的逻辑列批。
     *
     * @param session 当前检查点写会话
     * @param batchIndex 从一开始的连续列批序号
     * @param columns 当前列批字段
     * @param features 当前列批特征
     * @param clustering 当前列批聚类结果
     */
    void saveColumnBatch(SnapshotCheckpointWriteSession session,
                         int batchIndex,
                         List<String> columns,
                         FeatureAssemblyResult features,
                         ClusteringBatchResult clustering);

    /**
     * 校验全部列批并提交最终清单。
     */
    void complete(SnapshotCheckpointWriteSession session,
                  StrategyBatchResult strategyBatch);

    /**
     * 中止尚未提交最终清单的检查点，并清理已经写出的批次明细。
     */
    void abort(SnapshotCheckpointWriteSession session);

    default void save(String sourceJobId,
                      RahaDataset dataset,
                      DatasetSnapshot snapshot,
                      List<StrategyPlan> strategyPlans,
                      String strategyPlanVersion,
                      StrategyBatchResult strategyBatch,
                      FeatureAssemblyResult features,
                      ClusteringBatchResult clustering,
                      String configFingerprint,
                      long createdAt) {
        SnapshotCheckpointWriteSession session = begin(sourceJobId, dataset,
                snapshot, strategyPlans, strategyPlanVersion,
                configFingerprint, createdAt);
        List<String> columns = new java.util.ArrayList<String>(
                features.getDictionaries().keySet());
        java.util.Collections.sort(columns);
        saveColumnBatch(session, 1, columns, features, clustering);
        complete(session, strategyBatch);
    }

    /**
     * 清理超过保留期且已经完成的 HDFS 检查点明细。
     */
    default void cleanupExpired(long currentTimeMillis) {
        // 进程内仓储没有外部明细，无需清理。
    }

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
