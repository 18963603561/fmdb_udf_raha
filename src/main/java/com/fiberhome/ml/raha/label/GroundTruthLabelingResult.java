package com.fiberhome.ml.raha.label;

import com.fiberhome.ml.raha.sampling.AnnotationTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存真值适配器生成的直接标签和已完成标注任务。
 */
public final class GroundTruthLabelingResult {

    /** 已完成状态的标注任务。 */
    private final AnnotationTask completedTask;
    /** 当前元组全部可检测字段的零一标签。 */
    private final List<CellLabel> labels;

    public GroundTruthLabelingResult(AnnotationTask completedTask,
                                     List<CellLabel> labels) {
        if (completedTask == null || labels == null) {
            throw new IllegalArgumentException("真值标注结果参数不能为空");
        }
        this.completedTask = completedTask.snapshot();
        this.labels = Collections.unmodifiableList(new ArrayList<CellLabel>(labels));
    }

    public AnnotationTask getCompletedTask() { return completedTask.snapshot(); }
    public List<CellLabel> getLabels() { return labels; }
}
