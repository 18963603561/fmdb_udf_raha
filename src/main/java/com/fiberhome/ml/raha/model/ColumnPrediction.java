package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存列级模型对一个单元格输出的分数、阈值和模型类型。
 */
public final class ColumnPrediction {

    /** 单元格标识。 */
    private final String cellId;
    /** 错误概率或降级分数。 */
    private final double score;
    /** 判断阈值。 */
    private final double threshold;
    /** 是否疑似错误。 */
    private final boolean error;
    /** 分类器类型。 */
    private final ClassifierType classifierType;
    /** 模型版本。 */
    private final String modelVersion;

    public ColumnPrediction(String cellId,
                            double score,
                            double threshold,
                            boolean error,
                            ClassifierType classifierType,
                            String modelVersion) {
        this.cellId = ValueUtils.requireNotBlank(cellId, "单元格标识");
        if (Double.isNaN(score) || score < 0.0d || score > 1.0d
                || Double.isNaN(threshold) || threshold < 0.0d || threshold > 1.0d
                || classifierType == null) {
            throw new IllegalArgumentException("列级预测分数、阈值和类型非法");
        }
        this.score = score;
        this.threshold = threshold;
        this.error = error;
        this.classifierType = classifierType;
        this.modelVersion = ValueUtils.requireNotBlank(modelVersion, "模型版本");
    }

    public String getCellId() { return cellId; }
    public double getScore() { return score; }
    public double getThreshold() { return threshold; }
    public boolean isError() { return error; }
    public ClassifierType getClassifierType() { return classifierType; }
    public String getModelVersion() { return modelVersion; }
}
