package com.fiberhome.ml.raha.detection;

/**
 * 汇总检测单元格数量、疑似错误数量和平均分数。
 */
public final class DetectionMetrics {

    /** 生成结果的单元格数量。 */
    private final long detectedCellCount;
    /** 达到阈值的疑似错误数量。 */
    private final long errorCellCount;
    /** 全部单元格平均分数。 */
    private final double averageScore;

    public DetectionMetrics(long detectedCellCount, long errorCellCount, double averageScore) {
        if (detectedCellCount < 0L || errorCellCount < 0L || errorCellCount > detectedCellCount
                || Double.isNaN(averageScore) || averageScore < 0.0d || averageScore > 1.0d) {
            throw new IllegalArgumentException("检测指标非法");
        }
        this.detectedCellCount = detectedCellCount;
        this.errorCellCount = errorCellCount;
        this.averageScore = averageScore;
    }

    public long getDetectedCellCount() { return detectedCellCount; }
    public long getErrorCellCount() { return errorCellCount; }
    public double getAverageScore() { return averageScore; }
}
