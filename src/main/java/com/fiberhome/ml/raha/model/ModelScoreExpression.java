package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.feature.FeatureDictionary;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.exp;
import static org.apache.spark.sql.functions.length;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

/**
 * 将冻结逻辑回归模型转换为 Spark 列表达式，避免逐行收集预测数据。
 */
public final class ModelScoreExpression {

    /**
     * 构建错误概率表达式。
     *
     * @param model 列模型
     * @param rawValue 原始字段列
     * @return 概率列
     */
    public Column score(RahaColumnModel model, Column rawValue) {
        Column text = coalesce(rawValue.cast("string"), lit(""));
        double[] coefficients = model.getCoefficients();
        Column logit = lit(model.getIntercept());
        logit = logit.plus(when(trim(text).equalTo(""), lit(1.0d))
                .otherwise(lit(0.0d)).multiply(coefficients[0]));
        logit = logit.plus(when(text.rlike("[-+]?\\d+(\\.\\d+)?"), lit(1.0d))
                .otherwise(lit(0.0d)).multiply(coefficients[1]));
        logit = logit.plus(length(text).divide(lit(100.0d))
                .multiply(coefficients[2]));
        logit = logit.plus(when(text.rlike(".*\\d.*"), lit(1.0d))
                .otherwise(lit(0.0d)).multiply(coefficients[3]));
        logit = logit.plus(when(text.rlike(".*\\s.*"), lit(1.0d))
                .otherwise(lit(0.0d)).multiply(coefficients[4]));
        FeatureDictionary dictionary = model.getFeatureDictionary();
        for (int index = FeatureDictionary.FIXED_FEATURE_COUNT;
             index < dictionary.getFeatureNames().size(); index++) {
            String name = dictionary.getFeatureNames().get(index);
            String expected = name.substring("value=".length());
            logit = logit.plus(when(trim(text).equalTo(expected), lit(1.0d))
                    .otherwise(lit(0.0d)).multiply(coefficients[index]));
        }
        return lit(1.0d).divide(lit(1.0d).plus(exp(logit.multiply(lit(-1.0d)))));
    }
}
