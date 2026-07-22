package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于进程内映射保存采样快照检查点。
 */
public final class DefaultSnapshotCheckpointRepository
        implements SnapshotCheckpointRepository {

    /** 按数据集、快照和配置指纹索引的最新检查点。 */
    private final Map<String, SnapshotPreparedArtifacts> checkpoints =
            new LinkedHashMap<String, SnapshotPreparedArtifacts>();

    @Override
    public synchronized void save(String sourceJobId,
                                  RahaDataset dataset,
                                  DatasetSnapshot snapshot,
                                  List<StrategyPlan> strategyPlans,
                                  String strategyPlanVersion,
                                  StrategyBatchResult strategyBatch,
                                  FeatureAssemblyResult features,
                                  ClusteringBatchResult clustering,
                                  String configFingerprint,
                                  long createdAt) {
        String key = key(dataset.getDatasetId(), snapshot.getSnapshotId(),
                configFingerprint);
        String checkpointId = "snapshot_"
                + HashUtils.md5Hex(key + "|" + sourceJobId + "|" + createdAt);
        checkpoints.put(key, new SnapshotPreparedArtifacts(checkpointId,
                dataset, snapshot, strategyPlans, strategyBatch, features,
                strategyPlanVersion, clustering, configFingerprint));
    }

    @Override
    public synchronized Optional<SnapshotPreparedArtifacts> restore(
            String datasetId,
            String snapshotId,
            String configFingerprint) {
        return Optional.ofNullable(checkpoints.get(key(datasetId, snapshotId,
                configFingerprint)));
    }

    private static String key(String datasetId,
                              String snapshotId,
                              String configFingerprint) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        String fingerprint = ValueUtils.requireNotBlank(configFingerprint,
                "执行配置指纹");
        return dataset.length() + ":" + dataset
                + snapshot.length() + ":" + snapshot
                + fingerprint.length() + ":" + fingerprint;
    }
}
