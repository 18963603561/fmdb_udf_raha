package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.feature.FeatureVectorizer;

/**
 * 使用冻结字典和逻辑回归系数执行单元格预测。
 */
public final class LogisticRegressionColumnModelPredictor implements ColumnModelPredictor {

    /** 特征向量构建器。 */
    private final FeatureVectorizer vectorizer = new FeatureVectorizer();

    @Override
    public double predict(RahaColumnModel model, String value) {
        double[] features = vectorizer.vectorize(value, model.getFeatureDictionary());
        double score = model.getIntercept();
        double[] coefficients = model.getCoefficients();
        for (int index = 0; index < coefficients.length; index++) {
            score += coefficients[index] * features[index];
        }
        return 1.0d / (1.0d + Math.exp(-score));
    }
}
