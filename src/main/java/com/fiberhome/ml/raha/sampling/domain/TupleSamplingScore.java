package com.fiberhome.ml.raha.sampling.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一个元组的聚类覆盖分数及其字段贡献。
 */
public final class TupleSamplingScore {

    /** 稳定行标识。 */
    private final String rowId;
    /** 用于加权抽样的最终分数。 */
    private final double score;
    /** Python demo 语义下各列低覆盖贡献之和。 */
    private final double coverageScore;
    /** 当前行每个字段所属聚类。 */
    private final Map<String, String> coveredClusters;
    /** 每个字段对覆盖分数的贡献。 */
    private final Map<String, Double> columnContributions;

    public TupleSamplingScore(String rowId,
                              double score,
                              double coverageScore,
                              Map<String, String> coveredClusters,
                              Map<String, Double> columnContributions) {
        this.rowId = ValueUtils.requireNotBlank(rowId, "行标识");
        if (!finiteNonNegative(score) || !finiteNonNegative(coverageScore)) {
            throw new IllegalArgumentException("元组采样分数必须为非负有限数值");
        }
        if (coveredClusters == null || coveredClusters.isEmpty()
                || columnContributions == null) {
            throw new IllegalArgumentException("元组采样必须包含聚类覆盖和字段贡献");
        }
        this.score = score;
        this.coverageScore = coverageScore;
        this.coveredClusters = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(coveredClusters));
        this.columnContributions = Collections.unmodifiableMap(
                new LinkedHashMap<String, Double>(columnContributions));
    }

    private static boolean finiteNonNegative(double value) {
        return !Double.isNaN(value) && !Double.isInfinite(value) && value >= 0.0d;
    }

    public String getRowId() { return rowId; }
    public double getScore() { return score; }
    public double getCoverageScore() { return coverageScore; }
    public Map<String, String> getCoveredClusters() { return coveredClusters; }
    public Map<String, Double> getColumnContributions() { return columnContributions; }
}
