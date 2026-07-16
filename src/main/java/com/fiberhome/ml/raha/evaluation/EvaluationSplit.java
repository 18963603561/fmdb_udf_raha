package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.label.CellLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 保存排除直接标注坐标后的独立阈值验证集和最终测试集。
 */
public final class EvaluationSplit {

    /** 阈值选择使用的真值标签。 */
    private final List<CellLabel> validationLabels;
    /** 最终报告使用的真值标签。 */
    private final List<CellLabel> testLabels;
    /** 阈值验证集单元格标识。 */
    private final Set<String> validationCellIds;
    /** 最终测试集单元格标识。 */
    private final Set<String> testCellIds;

    public EvaluationSplit(List<CellLabel> validationLabels,
                           List<CellLabel> testLabels) {
        if (validationLabels == null || testLabels == null) {
            throw new IllegalArgumentException("评测验证集和测试集不能为空");
        }
        this.validationLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(validationLabels));
        this.testLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(testLabels));
        this.validationCellIds = cellIds(validationLabels);
        this.testCellIds = cellIds(testLabels);
        Set<String> overlap = new LinkedHashSet<String>(validationCellIds);
        overlap.retainAll(testCellIds);
        if (!overlap.isEmpty()) {
            throw new IllegalArgumentException("阈值验证集和最终测试集不能重叠");
        }
    }

    public List<CellLabel> getValidationLabels() { return validationLabels; }
    public List<CellLabel> getTestLabels() { return testLabels; }
    public Set<String> getValidationCellIds() { return validationCellIds; }
    public Set<String> getTestCellIds() { return testCellIds; }

    private static Set<String> cellIds(List<CellLabel> labels) {
        Set<String> values = new LinkedHashSet<String>();
        for (CellLabel label : labels) {
            if (label == null || !values.add(label.getCellId())) {
                throw new IllegalArgumentException("评测标签不能包含空值或重复单元格");
            }
        }
        return Collections.unmodifiableSet(values);
    }
}
