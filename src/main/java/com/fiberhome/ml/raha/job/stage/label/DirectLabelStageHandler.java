package com.fiberhome.ml.raha.job.stage.label;

import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
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
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        List<CellLabel> effectiveLabels = directLabels;
        Object existing = context.getAttributes().get(StageAttributeKeys.CELL_LABELS);
        // 持久化训练批次的标签已在合并阶段完成坐标转换，空调用方标签不能覆盖它们。
        if (effectiveLabels.isEmpty() && existing instanceof List
                && !((List<?>) existing).isEmpty()) {
            effectiveLabels = (List<CellLabel>) existing;
        }
        context.getAttributes().put(StageAttributeKeys.CELL_LABELS, effectiveLabels);
        return effectiveLabels.isEmpty()
                ? StageResult.skipped("当前训练任务没有直接标签")
                : StageResult.success();
    }
}
