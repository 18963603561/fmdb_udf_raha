package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.label.CellLabel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存脏表与真值表的全量单元格零一标签和类别数量。
 */
public final class GroundTruthDifferenceResult {

    /** 全部可检测单元格真值标签。 */
    private final List<CellLabel> labels;
    /** 错误单元格数量。 */
    private final long positiveCount;
    /** 正常单元格数量。 */
    private final long negativeCount;

    public GroundTruthDifferenceResult(List<CellLabel> labels,
                                       long positiveCount,
                                       long negativeCount) {
        if (labels == null || positiveCount < 0L || negativeCount < 0L
                || positiveCount + negativeCount != labels.size()) {
            throw new IllegalArgumentException("真值差异标签和类别数量不一致");
        }
        this.labels = Collections.unmodifiableList(new ArrayList<CellLabel>(labels));
        this.positiveCount = positiveCount;
        this.negativeCount = negativeCount;
    }

    public List<CellLabel> getLabels() { return labels; }
    public long getPositiveCount() { return positiveCount; }
    public long getNegativeCount() { return negativeCount; }
}
