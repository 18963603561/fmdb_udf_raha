package com.fiberhome.ml.raha.model.training;

import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存一个单元格的列级训练标签、稀疏特征和最终样本权重。
 */
public final class ColumnTrainingExample {

    /** 单元格标识。 */
    private final String cellId;
    /** 零一错误标签。 */
    private final int label;
    /** 标签来源。 */
    private final LabelSource labelSource;
    /** 非零稀疏特征。 */
    private final Map<Integer, Double> features;
    /** 标签基础权重与类别权重的乘积。 */
    private final double sampleWeight;

    public ColumnTrainingExample(String cellId,
                                 int label,
                                 LabelSource labelSource,
                                 Map<Integer, Double> features,
                                 double sampleWeight) {
        this.cellId = ValueUtils.requireNotBlank(cellId, "单元格标识");
        if ((label != 0 && label != 1) || labelSource == null
                || features == null || Double.isNaN(sampleWeight)
                || Double.isInfinite(sampleWeight) || sampleWeight <= 0.0d) {
            throw new IllegalArgumentException("训练样本标签、来源、特征和权重必须有效");
        }
        this.label = label;
        this.labelSource = labelSource;
        this.features = Collections.unmodifiableMap(
                new LinkedHashMap<Integer, Double>(features));
        this.sampleWeight = sampleWeight;
    }

    public String getCellId() { return cellId; }
    public int getLabel() { return label; }
    public LabelSource getLabelSource() { return labelSource; }
    public Map<Integer, Double> getFeatures() { return features; }
    public double getSampleWeight() { return sampleWeight; }
}
