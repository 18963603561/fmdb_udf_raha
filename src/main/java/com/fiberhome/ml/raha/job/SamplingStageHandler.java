package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.cluster.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.sampling.SamplingBatchResult;
import com.fiberhome.ml.raha.sampling.SamplingService;

import java.util.Collections;
import java.util.List;

/**
 * 根据列内聚类结果生成预算内的待标注元组任务。
 */
public final class SamplingStageHandler implements StageHandler {

    /** 主动采样服务。 */
    private final SamplingService samplingService;
    /** 当前采样轮次。 */
    private final int samplingRound;

    public SamplingStageHandler(SamplingService samplingService, int samplingRound) {
        if (samplingService == null || samplingRound <= 0) {
            throw new IllegalArgumentException("采样服务不能为空且轮次必须大于 0");
        }
        this.samplingService = samplingService;
        this.samplingRound = samplingRound;
    }

    @Override
    public StageType getStageType() {
        return StageType.SAMPLE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object clusteringValue = context.getAttributes().get(
                StageAttributeKeys.CLUSTERING_BATCH_RESULT);
        if (!(clusteringValue instanceof ClusteringBatchResult)) {
            return StageResult.failure("SAMPLING_INPUT_REQUIRED",
                    "采样阶段缺少列内聚类结果", false, 0L, 0L);
        }
        Object labelValue = context.getAttributes().get(StageAttributeKeys.CELL_LABELS);
        List<CellLabel> labels = labelValue instanceof List
                ? (List<CellLabel>) labelValue : Collections.<CellLabel>emptyList();
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        SamplingBatchResult result = samplingService.createTasks(
                context.getJob().getJobId(), samplingRound,
                (ClusteringBatchResult) clusteringValue, labels,
                context.getConfig().getSamplingConfig(), context.getConfig().getRandomSeed(),
                version);
        context.getAttributes().put(StageAttributeKeys.SAMPLING_BATCH_RESULT, result);
        context.getAttributes().put(StageAttributeKeys.ANNOTATION_TASKS, result.getTasks());
        return result.getTasks().isEmpty()
                ? StageResult.skipped("当前没有可生成的标注任务") : StageResult.success();
    }
}
