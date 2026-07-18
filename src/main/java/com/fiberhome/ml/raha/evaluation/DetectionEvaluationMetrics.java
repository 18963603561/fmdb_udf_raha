package com.fiberhome.ml.raha.evaluation;

/**
 * 保存单元格级混淆矩阵、精确率、召回率、F1 和平均精确率。
 */
public final class DetectionEvaluationMetrics {

    /** 参与评测的真值单元格数量。 */
    private final long evaluatedCellCount;
    /** 有显式检测分数的单元格数量。 */
    private final long scoredCellCount;
    /** 真正例数量。 */
    private final long truePositive;
    /** 假正例数量。 */
    private final long falsePositive;
    /** 假负例数量。 */
    private final long falseNegative;
    /** 真负例数量。 */
    private final long trueNegative;
    /** 单元格级精确率。 */
    private final double precision;
    /** 单元格级召回率。 */
    private final double recall;
    /** 精确率和召回率调和平均。 */
    private final double f1;
    /** 按错误分数排序的平均精确率。 */
    private final double averagePrecision;
    /** 本次统一评测阈值，混合模型结果评测时为空。 */
    private final Double threshold;

    public DetectionEvaluationMetrics(long evaluatedCellCount,
                                      long scoredCellCount,
                                      long truePositive,
                                      long falsePositive,
                                      long falseNegative,
                                      long trueNegative,
                                      double precision,
                                      double recall,
                                      double f1,
                                      double averagePrecision,
                                      Double threshold) {
        if (evaluatedCellCount < 0L || scoredCellCount < 0L
                || truePositive < 0L || falsePositive < 0L
                || falseNegative < 0L || trueNegative < 0L
                || truePositive + falsePositive + falseNegative + trueNegative
                != evaluatedCellCount) {
            throw new IllegalArgumentException("检测评测数量不一致");
        }
        validateMetric(precision);
        validateMetric(recall);
        validateMetric(f1);
        validateMetric(averagePrecision);
        if (threshold != null) {
            validateMetric(threshold);
        }
        this.evaluatedCellCount = evaluatedCellCount;
        this.scoredCellCount = scoredCellCount;
        this.truePositive = truePositive;
        this.falsePositive = falsePositive;
        this.falseNegative = falseNegative;
        this.trueNegative = trueNegative;
        this.precision = precision;
        this.recall = recall;
        this.f1 = f1;
        this.averagePrecision = averagePrecision;
        this.threshold = threshold;
    }

    public long getEvaluatedCellCount() { return evaluatedCellCount; }
    public long getScoredCellCount() { return scoredCellCount; }
    public long getTruePositive() { return truePositive; }
    public long getFalsePositive() { return falsePositive; }
    public long getFalseNegative() { return falseNegative; }
    public long getTrueNegative() { return trueNegative; }
    public double getPrecision() { return precision; }
    public double getRecall() { return recall; }
    public double getF1() { return f1; }
    public double getAveragePrecision() { return averagePrecision; }
    public Double getThreshold() { return threshold; }

    private static void validateMetric(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)
                || value < 0.0d || value > 1.0d) {
            throw new IllegalArgumentException("检测评测指标必须位于零到一之间");
        }
    }
}
