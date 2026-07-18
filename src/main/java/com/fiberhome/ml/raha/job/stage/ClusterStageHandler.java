package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.cluster.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.job.StageAttributeKeys;
import com.fiberhome.ml.raha.job.StageExecutionContext;
import com.fiberhome.ml.raha.job.StageResult;

/**
 * 对当前任务的单列稀疏特征执行聚类并持久化成员映射。
 */
public final class ClusterStageHandler implements StageHandler {

    /** 列内聚类服务。 */
    private final ColumnClusteringService clusteringService;

    public ClusterStageHandler(ColumnClusteringService clusteringService) {
        if (clusteringService == null) {
            throw new IllegalArgumentException("列内聚类服务不能为空");
        }
        this.clusteringService = clusteringService;
    }

    @Override
    public StageType getStageType() {
        return StageType.CLUSTER;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object featureValue = context.getAttributes().get(StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        if (!(featureValue instanceof FeatureAssemblyResult)) {
            return StageResult.failure("CLUSTER_INPUT_REQUIRED",
                    "聚类阶段缺少单元格特征", false, 0L, 0L);
        }
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        ClusteringBatchResult result = clusteringService.clusterAndSave(
                context.getJob().getJobId(), (FeatureAssemblyResult) featureValue,
                context.getConfig().getClusteringConfig(), context.getConfig().getRandomSeed(),
                version);
        context.getAttributes().put(StageAttributeKeys.CLUSTERING_BATCH_RESULT, result);
        return result.getMetrics().getAssignmentCount() == 0L
                ? StageResult.skipped("当前特征没有可用聚类成员") : StageResult.success();
    }
}
