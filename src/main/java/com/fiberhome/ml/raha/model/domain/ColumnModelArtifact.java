package com.fiberhome.ml.raha.model.domain;

import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存可移植列级线性模型参数，不依赖训练时 MLlib 对象生命周期。
 */
public final class ColumnModelArtifact {

    /** 模型逻辑名称。 */
    private final String modelName;
    /** 不可变模型版本。 */
    private final String modelVersion;
    /** 模型目标字段。 */
    private final String columnName;
    /** 分类器类型。 */
    private final ClassifierType classifierType;
    /** 特征字典版本。 */
    private final String featureDictionaryVersion;
    /** 特征维度。 */
    private final int featureDimension;
    /** 错误判断阈值。 */
    private final double threshold;
    /** 逻辑函数截距。 */
    private final double intercept;
    /** 按特征编号索引的线性系数。 */
    private final Map<Integer, Double> coefficients;
    /** 训练器或降级模式名称。 */
    private final String trainingMode;

    public ColumnModelArtifact(String modelName,
                               String modelVersion,
                               String columnName,
                               ClassifierType classifierType,
                               String featureDictionaryVersion,
                               int featureDimension,
                               double threshold,
                               double intercept,
                               Map<Integer, Double> coefficients,
                               String trainingMode) {
        this.modelName = ValueUtils.requireNotBlank(modelName, "模型名称");
        this.modelVersion = ValueUtils.requireNotBlank(modelVersion, "模型版本");
        this.columnName = ValueUtils.requireNotBlank(columnName, "模型字段");
        if (classifierType == null || featureDimension <= 0
                || Double.isNaN(threshold) || threshold < 0.0d || threshold > 1.0d
                || Double.isNaN(intercept) || Double.isInfinite(intercept)
                || coefficients == null) {
            throw new IllegalArgumentException("列级模型类型、维度、阈值和参数必须有效");
        }
        for (Map.Entry<Integer, Double> entry : coefficients.entrySet()) {
            if (entry.getKey() == null || entry.getKey() < 0
                    || entry.getKey() >= featureDimension || entry.getValue() == null
                    || Double.isNaN(entry.getValue()) || Double.isInfinite(entry.getValue())) {
                throw new IllegalArgumentException("列级模型系数编号和值非法");
            }
        }
        this.classifierType = classifierType;
        this.featureDictionaryVersion = ValueUtils.requireNotBlank(
                featureDictionaryVersion, "特征字典版本");
        this.featureDimension = featureDimension;
        this.threshold = threshold;
        this.intercept = intercept;
        this.coefficients = Collections.unmodifiableMap(
                new LinkedHashMap<Integer, Double>(coefficients));
        this.trainingMode = ValueUtils.requireNotBlank(trainingMode, "训练模式");
    }

    /**
     * 对同一字典下的稀疏特征计算逻辑概率。
     *
     * @param row 单元格稀疏特征
     * @return 零到一之间的错误概率
     */
    public double score(SparseFeatureRow row) {
        if (row == null || !columnName.equals(row.getColumnName())
                || !featureDictionaryVersion.equals(row.getFeatureDictionaryVersion())) {
            throw new IllegalArgumentException("预测特征与模型字段或字典版本不一致");
        }
        double linear = intercept;
        for (Map.Entry<Integer, Double> entry : row.getValues().entrySet()) {
            if (entry.getKey() >= featureDimension) {
                throw new IllegalArgumentException("预测特征编号超过模型维度");
            }
            Double coefficient = coefficients.get(entry.getKey());
            if (coefficient != null) {
                linear += coefficient * entry.getValue();
            }
        }
        if (linear >= 35.0d) {
            return 1.0d;
        }
        if (linear <= -35.0d) {
            return 0.0d;
        }
        return 1.0d / (1.0d + Math.exp(-linear));
    }

    public String getModelName() { return modelName; }
    public String getModelVersion() { return modelVersion; }
    public String getColumnName() { return columnName; }
    public ClassifierType getClassifierType() { return classifierType; }
    public String getFeatureDictionaryVersion() { return featureDictionaryVersion; }
    public int getFeatureDimension() { return featureDimension; }
    public double getThreshold() { return threshold; }
    public double getIntercept() { return intercept; }
    public Map<Integer, Double> getCoefficients() { return coefficients; }
    public String getTrainingMode() { return trainingMode; }

    /**
     * 使用发布元数据中的阈值创建参数相同的预测视图。
     *
     * @param selectedThreshold 已评测选定的生产阈值
     * @return 采用新阈值的不可变模型参数
     */
    public ColumnModelArtifact withThreshold(double selectedThreshold) {
        return new ColumnModelArtifact(modelName, modelVersion, columnName,
                classifierType, featureDictionaryVersion, featureDimension,
                selectedThreshold, intercept, coefficients, trainingMode);
    }
}
