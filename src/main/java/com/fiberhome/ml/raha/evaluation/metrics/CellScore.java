package com.fiberhome.ml.raha.evaluation.metrics;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存评测所需的单元格标识和错误分数。
 */
public final class CellScore {

    /** 稳定单元格标识。 */
    private final String cellId;
    /** 零到一之间的错误分数。 */
    private final double score;

    public CellScore(String cellId, double score) {
        this.cellId = ValueUtils.requireNotBlank(cellId, "评测单元格标识");
        if (Double.isNaN(score) || Double.isInfinite(score)
                || score < 0.0d || score > 1.0d) {
            throw new IllegalArgumentException("评测分数必须位于零到一之间");
        }
        this.score = score;
    }

    public String getCellId() { return cellId; }
    public double getScore() { return score; }
}
