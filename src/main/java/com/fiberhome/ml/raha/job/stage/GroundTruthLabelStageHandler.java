package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.GroundTruthLabelAdapter;
import com.fiberhome.ml.raha.label.GroundTruthLabelingResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import java.util.ArrayList;
import java.util.List;

/**
 * 在评测流水线中使用真值表完成待标注任务并生成直接标签。
 */
public final class GroundTruthLabelStageHandler implements StageHandler {

    /** 真值自动标注适配器。 */
    private final GroundTruthLabelAdapter adapter;
    /** 标注任务仓储。 */
    private final AnnotationTaskRepository taskRepository;
    /** 与脏表按行标识对齐的真值数据集。 */
    private final RahaDataset groundTruthDataset;

    public GroundTruthLabelStageHandler(GroundTruthLabelAdapter adapter,
                                        AnnotationTaskRepository taskRepository,
                                        RahaDataset groundTruthDataset) {
        if (adapter == null || taskRepository == null || groundTruthDataset == null) {
            throw new IllegalArgumentException("真值标注阶段依赖不能为空");
        }
        this.adapter = adapter;
        this.taskRepository = taskRepository;
        this.groundTruthDataset = groundTruthDataset;
    }

    @Override
    public StageType getStageType() {
        return StageType.LABEL;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object datasetValue = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        Object taskValue = context.getAttributes().get(StageAttributeKeys.ANNOTATION_TASKS);
        if (!(datasetValue instanceof RahaDataset) || !(taskValue instanceof List)) {
            return StageResult.failure("GROUND_TRUTH_INPUT_REQUIRED",
                    "真值标注阶段缺少脏数据集或标注任务", false, 0L, 0L);
        }
        List<AnnotationTask> tasks = (List<AnnotationTask>) taskValue;
        List<AnnotationTask> completedTasks = new ArrayList<AnnotationTask>();
        List<CellLabel> labels = new ArrayList<CellLabel>();
        for (AnnotationTask task : tasks) {
            GroundTruthLabelingResult result = adapter.label(
                    context.getConfig().getJobType(), task,
                    (RahaDataset) datasetValue, groundTruthDataset);
            completedTasks.add(result.getCompletedTask());
            labels.addAll(result.getLabels());
        }
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        long updatedAt = context.getStage().getStartedAt();
        for (AnnotationTask task : completedTasks) {
            updatedAt = Math.max(updatedAt, task.getFinishedAt());
        }
        taskRepository.saveAll(context.getJob().getJobId(), completedTasks,
                version, updatedAt);
        context.getAttributes().put(StageAttributeKeys.ANNOTATION_TASKS, completedTasks);
        context.getAttributes().put(StageAttributeKeys.CELL_LABELS, labels);
        return completedTasks.isEmpty()
                ? StageResult.skipped("当前没有待自动标注任务") : StageResult.success();
    }
}
