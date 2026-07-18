package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.feature.FeatureDictionary;

/**
 * 单个目标字段的不可变逻辑回归模型。
 */
public final class RahaColumnModel {

    /** 所属模型集合。 */
    private final String modelSetVersion;
    /** 数据集标识。 */
    private final String datasetId;
    /** 列模型版本。 */
    private final String modelVersion;
    /** 可选父列模型版本。 */
    private final String parentModelVersion;
    /** 目标字段。 */
    private final String columnName;
    /** 分类器类型。 */
    private final ClassifierType classifierType;
    /** 冻结特征字典。 */
    private final FeatureDictionary featureDictionary;
    /** 错误判断阈值。 */
    private final double threshold;
    /** 逻辑回归截距。 */
    private final double intercept;
    /** 逻辑回归系数。 */
    private final double[] coefficients;
    /** 训练摘要 JSON。 */
    private final String trainingSummaryJson;
    /** 模型集合提交时间。 */
    private final long createdAt;

    public RahaColumnModel(String modelSetVersion, String datasetId, String modelVersion,
                           String parentModelVersion, String columnName,
                           FeatureDictionary featureDictionary, double threshold,
                           double intercept, double[] coefficients,
                           String trainingSummaryJson, long createdAt) {
        this.modelSetVersion = modelSetVersion;
        this.datasetId = datasetId;
        this.modelVersion = modelVersion;
        this.parentModelVersion = parentModelVersion;
        this.columnName = columnName;
        this.classifierType = ClassifierType.LOGISTIC_REGRESSION;
        this.featureDictionary = featureDictionary;
        this.threshold = threshold;
        this.intercept = intercept;
        this.coefficients = coefficients.clone();
        this.trainingSummaryJson = trainingSummaryJson;
        this.createdAt = createdAt;
    }

    public String getModelSetVersion() { return modelSetVersion; }
    public String getDatasetId() { return datasetId; }
    public String getModelVersion() { return modelVersion; }
    public String getParentModelVersion() { return parentModelVersion; }
    public String getColumnName() { return columnName; }
    public ClassifierType getClassifierType() { return classifierType; }
    public FeatureDictionary getFeatureDictionary() { return featureDictionary; }
    public double getThreshold() { return threshold; }
    public double getIntercept() { return intercept; }
    public double[] getCoefficients() { return coefficients.clone(); }
    public String getTrainingSummaryJson() { return trainingSummaryJson; }
    public long getCreatedAt() { return createdAt; }
}
