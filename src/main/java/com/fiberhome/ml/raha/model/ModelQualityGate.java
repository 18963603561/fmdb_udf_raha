package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 评估训练集分数分布并阻止恒定分数或全同类别模型进入候选状态。
 */
public final class ModelQualityGate {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(ModelQualityGate.class);

    private ModelQualityGate() {
    }

    /**
     * 计算训练质量指标，并在启用门禁时将退化模型转换为失败结果。
     *
     * @param result 原始训练结果
     * @param dataset 当前字段训练集
     * @param config 模型与门禁配置
     * @return 已补充质量指标的训练结果
     */
    public static ColumnModelTrainingResult evaluate(ColumnModelTrainingResult result,
                                                      ColumnTrainingDataset dataset,
                                                      ModelConfig config) {
        if (result == null || dataset == null || config == null) {
            throw new IllegalArgumentException("模型质量门禁输入不能为空");
        }
        if (result.getStatus() != ColumnModelTrainingStatus.TRAINED) {
            return result;
        }
        Map<String, Double> metrics = metrics(result.getArtifact(), dataset);
        ColumnModelTrainingResult measured = result.withMetrics(metrics);
        if (!config.isQualityGateEnabled()) {
            return measured;
        }
        double scoreStddev = metrics.get("scoreStandardDeviation");
        double positiveRatio = metrics.get("predictedPositiveRatio");
        double f1 = metrics.get("trainingF1");
        double minimumPositiveRatio = 1.0d - config.getMaximumPositiveRatio();
        boolean rejected = scoreStddev < config.getMinimumScoreStandardDeviation()
                || positiveRatio <= minimumPositiveRatio
                || positiveRatio >= config.getMaximumPositiveRatio()
                || f1 < config.getMinimumF1();
        if (!rejected) {
            return measured;
        }
        LOGGER.warn("模型质量门禁拒绝退化模型，columnName={}，scoreStddev={}，"
                        + "positiveRatio={}，trainingF1={}",
                dataset.getColumnName(), scoreStddev, positiveRatio, f1);
        Map<String, Double> rejectedMetrics = new LinkedHashMap<String, Double>(
                measured.getMetrics());
        rejectedMetrics.put("qualityGatePassed", 0.0d);
        return new ColumnModelTrainingResult(ColumnModelTrainingStatus.FAILED,
                null, result.isFallback(), "模型质量门禁拒绝退化模型", rejectedMetrics);
    }

    private static Map<String, Double> metrics(ColumnModelArtifact artifact,
                                               ColumnTrainingDataset dataset) {
        double minimum = Double.POSITIVE_INFINITY;
        double maximum = Double.NEGATIVE_INFINITY;
        double sum = 0.0d;
        double squaredSum = 0.0d;
        int predictedPositive = 0;
        int truePositive = 0;
        int falsePositive = 0;
        int falseNegative = 0;
        for (ColumnTrainingExample example : dataset.getExamples()) {
            SparseFeatureRow row = new SparseFeatureRow(example.getCellId(),
                    dataset.getColumnName(), dataset.getFeatureDictionaryVersion(),
                    example.getFeatures(), Collections.<String, String>emptyMap());
            double score = artifact.score(row);
            boolean predicted = score >= artifact.getThreshold();
            minimum = Math.min(minimum, score);
            maximum = Math.max(maximum, score);
            sum += score;
            squaredSum += score * score;
            if (predicted) {
                predictedPositive++;
            }
            if (predicted && example.getLabel() == 1) {
                truePositive++;
            } else if (predicted) {
                falsePositive++;
            } else if (example.getLabel() == 1) {
                falseNegative++;
            }
        }
        int count = dataset.getExamples().size();
        double mean = sum / count;
        double variance = Math.max(0.0d, squaredSum / count - mean * mean);
        double precision = truePositive + falsePositive == 0 ? 0.0d
                : (double) truePositive / (truePositive + falsePositive);
        double recall = truePositive + falseNegative == 0 ? 0.0d
                : (double) truePositive / (truePositive + falseNegative);
        double f1 = precision + recall == 0.0d ? 0.0d
                : 2.0d * precision * recall / (precision + recall);
        Map<String, Double> metrics = new LinkedHashMap<String, Double>();
        metrics.put("scoreMinimum", minimum);
        metrics.put("scoreMaximum", maximum);
        metrics.put("scoreMean", mean);
        metrics.put("scoreStandardDeviation", Math.sqrt(variance));
        metrics.put("predictedPositiveRatio", (double) predictedPositive / count);
        metrics.put("trainingPrecision", precision);
        metrics.put("trainingRecall", recall);
        metrics.put("trainingF1", f1);
        metrics.put("qualityGatePassed", 1.0d);
        return metrics;
    }
}
