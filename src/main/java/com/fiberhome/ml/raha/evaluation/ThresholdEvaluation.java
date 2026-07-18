package com.fiberhome.ml.raha.evaluation;

/**
 * 保存一个候选阈值及其完整检测评测指标。
 */
public final class ThresholdEvaluation {

    /** 候选判断阈值。 */
    private final double threshold;
    /** 当前阈值的评测指标。 */
    private final DetectionEvaluationMetrics metrics;

    public ThresholdEvaluation(double threshold,
                               DetectionEvaluationMetrics metrics) {
        if (Double.isNaN(threshold) || threshold < 0.0d || threshold > 1.0d
                || metrics == null) {
            throw new IllegalArgumentException("候选阈值和评测指标必须有效");
        }
        this.threshold = threshold;
        this.metrics = metrics;
    }

    public double getThreshold() { return threshold; }
    public DetectionEvaluationMetrics getMetrics() { return metrics; }
}
