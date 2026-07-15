package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ClassifierType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据首选分类器和 MLlib 训练结果选择逻辑回归或明确标记的降级训练器。
 */
public final class AdaptiveColumnModelTrainer implements ColumnModelTrainer {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AdaptiveColumnModelTrainer.class);
    /** Spark MLlib 逻辑回归训练器。 */
    private final ColumnModelTrainer mllibTrainer;
    /** 规则加权降级训练器。 */
    private final ColumnModelTrainer fallbackTrainer;
    /** 决策树训练器，可为空表示当前装配未启用。 */
    private final ColumnModelTrainer decisionTreeTrainer;
    /** 梯度提升树训练器，可为空表示当前装配未启用。 */
    private final ColumnModelTrainer gbtTrainer;

    public AdaptiveColumnModelTrainer(ColumnModelTrainer mllibTrainer,
                                      ColumnModelTrainer fallbackTrainer) {
        this(mllibTrainer, fallbackTrainer, null, null);
    }

    public AdaptiveColumnModelTrainer(ColumnModelTrainer mllibTrainer,
                                      ColumnModelTrainer fallbackTrainer,
                                      ColumnModelTrainer decisionTreeTrainer,
                                      ColumnModelTrainer gbtTrainer) {
        if (mllibTrainer == null || fallbackTrainer == null) {
            throw new IllegalArgumentException("自适应训练器依赖不能为空");
        }
        this.mllibTrainer = mllibTrainer;
        this.fallbackTrainer = fallbackTrainer;
        this.decisionTreeTrainer = decisionTreeTrainer;
        this.gbtTrainer = gbtTrainer;
    }

    @Override
    public ColumnModelTrainingResult train(ColumnModelTrainingRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("列级模型训练请求不能为空");
        }
        if (request.getDataset().getStatus() != ColumnTrainingStatus.TRAINABLE) {
            return ColumnModelTrainingResult.untrainable(request.getDataset());
        }
        ClassifierType preferred = request.getModelConfig().getClassifierType();
        if (preferred == ClassifierType.WEIGHTED_RULE) {
            LOGGER.info("按配置使用规则加权训练器，datasetId={}，columnName={}",
                    request.getDatasetId(), request.getDataset().getColumnName());
            return fallbackTrainer.train(request);
        }
        if (preferred == ClassifierType.LOGISTIC_REGRESSION) {
            ColumnModelTrainingResult result = mllibTrainer.train(request);
            if (result.getStatus() == ColumnModelTrainingStatus.TRAINED
                    || !request.getModelConfig().isFallbackEnabled()) {
                return result;
            }
            LOGGER.warn("MLlib 训练未成功，准备使用规则加权降级，datasetId={}，columnName={}，status={}",
                    request.getDatasetId(), request.getDataset().getColumnName(),
                    result.getStatus());
            return fallbackTrainer.train(request);
        }
        if (preferred == ClassifierType.DECISION_TREE && decisionTreeTrainer != null) {
            return trainWithFallback(request, decisionTreeTrainer, "决策树");
        }
        if (preferred == ClassifierType.GBT && gbtTrainer != null) {
            return trainWithFallback(request, gbtTrainer, "梯度提升树");
        }
        if (request.getModelConfig().isFallbackEnabled()) {
            LOGGER.warn("首选分类器尚未实现，使用规则加权降级，datasetId={}，"
                            + "columnName={}，classifierType={}",
                    request.getDatasetId(), request.getDataset().getColumnName(), preferred);
            return fallbackTrainer.train(request);
        }
        LOGGER.warn("首选分类器未实现且禁止降级，datasetId={}，columnName={}，"
                        + "classifierType={}",
                request.getDatasetId(), request.getDataset().getColumnName(), preferred);
        return new ColumnModelTrainingResult(ColumnModelTrainingStatus.FAILED,
                null, false, "首选分类器未实现且禁止规则降级", null);
    }

    private ColumnModelTrainingResult trainWithFallback(ColumnModelTrainingRequest request,
                                                        ColumnModelTrainer trainer,
                                                        String classifierName) {
        ColumnModelTrainingResult result = trainer.train(request);
        if (result.getStatus() == ColumnModelTrainingStatus.TRAINED
                || !request.getModelConfig().isFallbackEnabled()) {
            return result;
        }
        LOGGER.warn("{}训练未成功，准备使用规则加权降级，datasetId={}，columnName={}，status={}",
                classifierName, request.getDatasetId(), request.getDataset().getColumnName(),
                result.getStatus());
        return fallbackTrainer.train(request);
    }
}
