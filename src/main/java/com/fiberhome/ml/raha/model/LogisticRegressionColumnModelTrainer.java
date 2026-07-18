package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;
import com.fiberhome.ml.raha.train.TrainingExample;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Spark ML 逻辑回归训练实现，训练后选择训练集 F1 最优阈值。
 */
public final class LogisticRegressionColumnModelTrainer implements ColumnModelTrainer {

    /** 当前 Spark 会话。 */
    private final SparkSession sparkSession;
    /** 逻辑回归参数。 */
    private final ModelConfig config;

    public LogisticRegressionColumnModelTrainer(SparkSession sparkSession, ModelConfig config) {
        this.sparkSession = sparkSession;
        this.config = config;
    }

    @Override
    public RahaColumnModel train(String modelSetVersion, String datasetId,
                                 String columnName, String parentModelVersion,
                                 FeatureDictionary dictionary,
                                 List<TrainingExample> examples, long createdAt) {
        int positive = 0;
        int negative = 0;
        List<Row> rows = new ArrayList<Row>();
        for (TrainingExample example : examples) {
            if (example.getLabel() == 1) {
                positive++;
            } else {
                negative++;
            }
            rows.add(RowFactory.create((double) example.getLabel(),
                    Vectors.dense(example.getFeatureVector()), example.getSampleWeight()));
        }
        if (positive == 0 || negative == 0) {
            throw new RahaException(RahaErrorCode.INVALID_DATA,
                    "字段训练样本必须同时包含正常和错误类别：" + columnName);
        }
        StructType schema = new StructType()
                .add("label", DataTypes.DoubleType, false)
                .add("features", SQLDataTypes.VectorType(), false)
                .add("weight", DataTypes.DoubleType, false);
        Dataset<Row> frame = sparkSession.createDataFrame(rows, schema);
        try {
            LogisticRegression estimator = new LogisticRegression()
                    .setLabelCol("label")
                    .setFeaturesCol("features")
                    .setWeightCol("weight")
                    .setFamily("binomial")
                    .setMaxIter(config.getMaxIterations())
                    .setRegParam(config.getRegularization())
                    .setElasticNetParam(0.0d);
            LogisticRegressionModel trained = estimator.fit(frame);
            double[] coefficients = trained.coefficients().toArray();
            double intercept = trained.intercept();
            double threshold = selectThreshold(intercept, coefficients, examples,
                    config.getDefaultThreshold());
            Map<String, Object> summary = new LinkedHashMap<String, Object>();
            summary.put("examples", examples.size());
            summary.put("positive", positive);
            summary.put("negative", negative);
            summary.put("threshold", threshold);
            String signature = dictionary.getVersion() + '|'
                    + JsonUtils.toJson(coefficients) + '|' + intercept + '|' + threshold;
            return new RahaColumnModel(modelSetVersion, datasetId,
                    "model_" + HashUtils.sha256(columnName + '|' + signature)
                            .substring(0, 24), parentModelVersion, columnName,
                    dictionary, threshold, intercept, coefficients,
                    JsonUtils.toJson(summary), createdAt);
        } catch (RuntimeException exception) {
            throw new RahaException(RahaErrorCode.ALGORITHM_ERROR,
                    "逻辑回归训练失败，字段=" + columnName, exception);
        }
    }

    private static double selectThreshold(double intercept, double[] coefficients,
                                          List<TrainingExample> examples,
                                          double defaultThreshold) {
        List<Double> candidates = new ArrayList<Double>();
        candidates.add(defaultThreshold);
        for (TrainingExample example : examples) {
            candidates.add(score(intercept, coefficients, example.getFeatureVector()));
        }
        Collections.sort(candidates, Comparator.naturalOrder());
        double bestThreshold = defaultThreshold;
        double bestF1 = -1.0d;
        for (double candidate : candidates) {
            long truePositive = 0;
            long falsePositive = 0;
            long falseNegative = 0;
            for (TrainingExample example : examples) {
                boolean predicted = score(intercept, coefficients,
                        example.getFeatureVector()) >= candidate;
                if (predicted && example.getLabel() == 1) {
                    truePositive++;
                } else if (predicted) {
                    falsePositive++;
                } else if (example.getLabel() == 1) {
                    falseNegative++;
                }
            }
            double precision = truePositive == 0 ? 0.0d
                    : truePositive / (double) (truePositive + falsePositive);
            double recall = truePositive == 0 ? 0.0d
                    : truePositive / (double) (truePositive + falseNegative);
            double f1 = precision + recall == 0.0d ? 0.0d
                    : 2.0d * precision * recall / (precision + recall);
            if (f1 > bestF1 || (f1 == bestF1 && candidate > bestThreshold)) {
                bestF1 = f1;
                bestThreshold = candidate;
            }
        }
        return Math.max(0.000001d, Math.min(0.999999d, bestThreshold));
    }

    private static double score(double intercept, double[] coefficients, double[] features) {
        double value = intercept;
        for (int index = 0; index < coefficients.length; index++) {
            value += coefficients[index] * features[index];
        }
        return 1.0d / (1.0d + Math.exp(-value));
    }
}
