package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.label.CellLabel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 将调用方提供的直接标签写入任务阶段上下文。
 */
public final class DirectLabelStageHandler implements StageHandler {

    /** 训练任务使用的人工、真值或规则确认标签。 */
    private final List<CellLabel> directLabels;

    public DirectLabelStageHandler(List<CellLabel> directLabels) {
        if (directLabels == null) {
            throw new IllegalArgumentException("直接标签列表不能为空");
        }
        for (CellLabel label : directLabels) {
            if (label == null || label.getLabelSource() == LabelSource.PROPAGATED) {
                throw new IllegalArgumentException("标签阶段只接受直接标签");
            }
        }
        this.directLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(directLabels));
    }

    @Override
    public StageType getStageType() {
        return StageType.LABEL;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        context.getAttributes().put(StageAttributeKeys.CELL_LABELS, directLabels);
        return directLabels.isEmpty()
                ? StageResult.skipped("当前训练任务没有直接标签")
                : StageResult.success();
    }
}
