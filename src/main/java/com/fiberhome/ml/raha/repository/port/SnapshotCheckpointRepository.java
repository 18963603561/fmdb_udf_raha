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

    Optional<SnapshotPreparedArtifacts> restore(String datasetId,
                                                String snapshotId,
                                                String configFingerprint);
}
