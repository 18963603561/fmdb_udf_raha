package com.fiberhome.ml.raha.label;

import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存单元格二分类标签及其来源，传播标签不会覆盖直接标签。
 */
public final class CellLabel {

    /** 标签稳定标识。 */
    private final String labelId;
    /** 稳定单元格标识。 */
    private final String cellId;
    /** 标签值，一表示疑似错误，零表示正常。 */
    private final int label;
    /** 标签来源。 */
    private final LabelSource labelSource;
    /** 标签置信度。 */
    private final double confidence;
    /** 传播标签对应的直接来源标签标识。 */
    private final String sourceLabelId;
    /** 传播标签所在的聚类标识。 */
    private final String clusterId;
    /** 传播标签使用的聚类版本。 */
    private final String clusterVersion;
    /** 传播冲突处理方式，直接标签为空。 */
    private final LabelPropagationMethod propagationMethod;
    /** 训练时使用的标签样本权重。 */
    private final double sampleWeight;
    /** 当前聚类中与传播结果冲突的直接标签数量。 */
    private final int conflictCount;
    /** 多数标签比例，同质传播和直接标签允许为空。 */
    private final Double majorityRatio;
    /** 标注人员或系统。 */
    private final String annotator;
    /** 标签创建时间。 */
    private final long createdAt;

    public CellLabel(String cellId,
                     int label,
                     LabelSource labelSource,
                     double confidence,
                     String sourceLabelId,
                     String clusterId,
                     String annotator,
                     long createdAt) {
        this(defaultLabelId(cellId, label, labelSource, createdAt), cellId, label,
                labelSource, confidence, sourceLabelId, clusterId,
                labelSource == LabelSource.PROPAGATED ? "LEGACY" : null,
                labelSource == LabelSource.PROPAGATED
                        ? LabelPropagationMethod.HOMOGENEITY : null,
                labelSource == LabelSource.PROPAGATED
                        ? Math.max(0.000001d, Math.min(0.5d, confidence)) : 1.0d,
                0, null, annotator, createdAt);
    }

    public CellLabel(String labelId,
                     String cellId,
                     int label,
                     LabelSource labelSource,
                     double confidence,
                     String sourceLabelId,
                     String clusterId,
                     String clusterVersion,
                     LabelPropagationMethod propagationMethod,
                     double sampleWeight,
                     int conflictCount,
                     Double majorityRatio,
                     String annotator,
                     long createdAt) {
        this.labelId = ValueUtils.requireNotBlank(labelId, "标签标识");
        this.cellId = ValueUtils.requireNotBlank(cellId, "单元格标识");
        if (label != 0 && label != 1) {
            throw new IllegalArgumentException("单元格标签只能为 0 或 1");
        }
        if (labelSource == null) {
            throw new IllegalArgumentException("标签来源不能为空");
        }
        if (Double.isNaN(confidence) || confidence < 0.0d || confidence > 1.0d) {
            throw new IllegalArgumentException("标签置信度必须位于 0 到 1 之间");
        }
        if (Double.isNaN(sampleWeight) || Double.isInfinite(sampleWeight)
                || sampleWeight <= 0.0d || sampleWeight > 1.0d) {
            throw new IllegalArgumentException("标签样本权重必须位于 0 到 1 之间且大于 0");
        }
        if (conflictCount < 0 || (majorityRatio != null
                && (Double.isNaN(majorityRatio) || majorityRatio < 0.5d
                || majorityRatio > 1.0d))) {
            throw new IllegalArgumentException("标签冲突数量或多数比例非法");
        }
        // 传播标签必须保存直接来源和聚类，防止扩展标签被误认为人工真值。
        if (labelSource == LabelSource.PROPAGATED
                && (isBlank(sourceLabelId) || isBlank(clusterId)
                || isBlank(clusterVersion) || propagationMethod == null)) {
            throw new IllegalArgumentException("传播标签必须包含来源、聚类版本和传播方式");
        }
        if (labelSource != LabelSource.PROPAGATED
                && (propagationMethod != null || !isBlank(clusterVersion)
                || conflictCount != 0 || majorityRatio != null)) {
            throw new IllegalArgumentException("直接标签不能携带传播统计");
        }
        if (createdAt <= 0L) {
            throw new IllegalArgumentException("标签创建时间必须大于 0");
        }
        this.label = label;
        this.labelSource = labelSource;
        this.confidence = confidence;
        this.sourceLabelId = sourceLabelId;
        this.clusterId = clusterId;
        this.clusterVersion = clusterVersion;
        this.propagationMethod = propagationMethod;
        this.sampleWeight = sampleWeight;
        this.conflictCount = conflictCount;
        this.majorityRatio = majorityRatio;
        this.annotator = annotator;
        this.createdAt = createdAt;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String defaultLabelId(String cellId,
                                         int label,
                                         LabelSource source,
                                         long createdAt) {
        return HashUtils.sha256Hex(String.valueOf(cellId) + "|" + label + "|"
                + String.valueOf(source) + "|" + createdAt);
    }

    public String getLabelId() {
        return labelId;
    }

    public String getCellId() {
        return cellId;
    }

    public int getLabel() {
        return label;
    }

    public LabelSource getLabelSource() {
        return labelSource;
    }

    public double getConfidence() {
        return confidence;
    }

    public String getSourceLabelId() {
        return sourceLabelId;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getClusterVersion() {
        return clusterVersion;
    }

    public LabelPropagationMethod getPropagationMethod() {
        return propagationMethod;
    }

    public double getSampleWeight() {
        return sampleWeight;
    }

    public int getConflictCount() {
        return conflictCount;
    }

    public Double getMajorityRatio() {
        return majorityRatio;
    }

    public String getAnnotator() {
        return annotator;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
