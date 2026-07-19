package com.fiberhome.ml.raha.job.stage.label;

import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationResult;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import java.util.ArrayList;
import java.util.List;

/**
 * 使用当前聚类和直接标签生成可复用传播结果。
 */
public final class LabelPropagationStageHandler implements StageHandler {

    /** 标签传播服务。 */
    private final LabelPropagationService propagationService;
    /** 当前训练任务的传播方式。 */
    private final LabelPropagationMethod method;
    /** 当前训练任务的传播配置。 */
    private final LabelPropagationConfig config;

    public LabelPropagationStageHandler(LabelPropagationService propagationService,
                                        LabelPropagationMethod method,
                                        LabelPropagationConfig config) {
        if (propagationService == null || method == null || config == null) {
            throw new IllegalArgumentException("标签传播服务、方式和配置不能为空");
        }
        this.propagationService = propagationService;
        this.method = method;
        this.config = config;
    }

    @Override
    public StageType getStageType() {
        return StageType.PROPAGATE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object clusteringValue = context.getAttributes().get(
                StageAttributeKeys.CLUSTERING_BATCH_RESULT);
        Object labelValue = context.getAttributes().get(StageAttributeKeys.CELL_LABELS);
        if (!(clusteringValue instanceof ClusteringBatchResult)
                || !(labelValue instanceof List)) {
            return StageResult.failure("PROPAGATION_INPUT_REQUIRED",
                    "标签传播阶段缺少聚类结果或直接标签", false, 0L, 0L);
        }
        List<CellLabel> labels = (List<CellLabel>) labelValue;
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        LabelPropagationResult result = propagationService.propagateAndSave(
                context.getJob().getJobId(), assignments(
                        (ClusteringBatchResult) clusteringValue), labels,
                method, config, version);
        context.getAttributes().put(StageAttributeKeys.LABEL_PROPAGATION_RESULT, result);
        return StageResult.success();
    }

    private static List<ClusterAssignment> assignments(ClusteringBatchResult clustering) {
        List<ClusterAssignment> assignments = new ArrayList<ClusterAssignment>();
        for (ColumnClusteringResult result : clustering.getResults().values()) {
            assignments.addAll(result.getAssignments());
        }
        return assignments;
    }
}
