package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ClassifierType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 在 MLlib 不可用时根据标签和特征加权均值生成可解释线性降级模型。
 */
public final class WeightedRuleFallbackTrainer implements ColumnModelTrainer {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            WeightedRuleFallbackTrainer.class);
    /** 模型版本生成器。 */
    private final ColumnModelVersioner versioner;

    public WeightedRuleFallbackTrainer(ColumnModelVersioner versioner) {
        if (versioner == null) {
            throw new IllegalArgumentException("模型版本生成器不能为空");
        }
        this.versioner = versioner;
    }

    @Override
    public ColumnModelTrainingResult train(ColumnModelTrainingRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("列级模型训练请求不能为空");
        }
        ColumnTrainingDataset dataset = request.getDataset();
        if (dataset.getStatus() != ColumnTrainingStatus.TRAINABLE) {
            return ColumnModelTrainingResult.untrainable(dataset);
        }
        LOGGER.info("开始规则加权降级训练，datasetId={}，columnName={}，sampleCount={}",
                request.getDatasetId(), dataset.getColumnName(), dataset.getExamples().size());
        double positiveWeight = 0.0d;
        double negativeWeight = 0.0d;
        double[] positiveSums = new double[dataset.getFeatureDimension()];
        double[] negativeSums = new double[dataset.getFeatureDimension()];
        for (ColumnTrainingExample example : dataset.getExamples()) {
            if (example.getLabel() == 1) {
                positiveWeight += example.getSampleWeight();
            } else {
                negativeWeight += example.getSampleWeight();
            }
            for (Map.Entry<Integer, Double> entry : example.getFeatures().entrySet()) {
                if (example.getLabel() == 1) {
                    positiveSums[entry.getKey()] += entry.getValue() * example.getSampleWeight();
                } else {
                    negativeSums[entry.getKey()] += entry.getValue() * example.getSampleWeight();
                }
            }
        }
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        for (int index = 0; index < dataset.getFeatureDimension(); index++) {
            double positiveMean = positiveWeight == 0.0d ? 0.0d
                    : positiveSums[index] / positiveWeight;
            double negativeMean = negativeWeight == 0.0d ? 0.0d
                    : negativeSums[index] / negativeWeight;
            double coefficient = positiveMean - negativeMean;
            if (Double.compare(coefficient, 0.0d) != 0) {
                coefficients.put(index, coefficient);
            }
        }
        double intercept = Math.log((positiveWeight + 0.000001d)
                / (negativeWeight + 0.000001d));
        String mode = "fallback_weighted_feature_rule";
        String modelVersion = versioner.versionOf(request,
                ClassifierType.WEIGHTED_RULE, intercept, coefficients, mode);
        ColumnModelArtifact artifact = new ColumnModelArtifact(request.getModelName(),
                modelVersion, dataset.getColumnName(), ClassifierType.WEIGHTED_RULE,
                dataset.getFeatureDictionaryVersion(), dataset.getFeatureDimension(),
                request.getModelConfig().getThreshold(), intercept, coefficients, mode);
        Map<String, Double> metrics = new LinkedHashMap<String, Double>();
        metrics.put("sampleCount", (double) dataset.getExamples().size());
        metrics.put("positiveCount", (double) dataset.getPositiveCount());
        metrics.put("negativeCount", (double) dataset.getNegativeCount());
        LOGGER.info("规则加权降级训练完成，datasetId={}，columnName={}，modelVersion={}",
                request.getDatasetId(), dataset.getColumnName(), modelVersion);
        return new ColumnModelTrainingResult(ColumnModelTrainingStatus.TRAINED,
                artifact, true, "使用规则加权降级训练器", metrics);
    }
}
