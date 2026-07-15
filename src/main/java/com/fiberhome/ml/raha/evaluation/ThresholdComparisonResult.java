package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.model.RahaColumnModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存全部候选阈值评测、选中阈值和已更新模型元数据。
 */
public final class ThresholdComparisonResult {

    /** 按阈值升序保存的评测结果。 */
    private final List<ThresholdEvaluation> evaluations;
    /** 确定性规则选中的阈值。 */
    private final double selectedThreshold;
    /** 已写入选中阈值和指标的模型元数据。 */
    private final RahaColumnModel updatedModel;

    public ThresholdComparisonResult(List<ThresholdEvaluation> evaluations,
                                     double selectedThreshold,
                                     RahaColumnModel updatedModel) {
        if (evaluations == null || evaluations.isEmpty() || updatedModel == null) {
            throw new IllegalArgumentException("阈值比较结果不能为空");
        }
        this.evaluations = Collections.unmodifiableList(
                new ArrayList<ThresholdEvaluation>(evaluations));
        this.selectedThreshold = selectedThreshold;
        this.updatedModel = updatedModel;
    }

    public List<ThresholdEvaluation> getEvaluations() { return evaluations; }
    public double getSelectedThreshold() { return selectedThreshold; }
    public RahaColumnModel getUpdatedModel() { return updatedModel; }
}
