package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.job.stage.core.StageEvaluator;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存统一任务入口执行训练、检测或采样所需的类型化输入。
 */
public final class RahaTaskExecutionRequest {

    /** 完整任务配置。 */
    private final RahaJobConfig config;
    /** 数据加载请求。 */
    private final DataLoadRequest dataLoadRequest;
    /** 训练或采样已经持有的标签。 */
    private final List<CellLabel> labels;
    /** 训练标签传播方式。 */
    private final LabelPropagationMethod propagationMethod;
    /** 训练标签传播配置。 */
    private final LabelPropagationConfig propagationConfig;
    /** 列模型训练配置。 */
    private final LogisticRegressionTrainingConfig trainingConfig;
    /** 候选模型名称前缀。 */
    private final String modelNamePrefix;
    /** 当前采样轮次。 */
    private final int samplingRound;
    /** 可选模型或检测评估器。 */
    private final StageEvaluator evaluator;

    private RahaTaskExecutionRequest(RahaJobConfig config,
                                     DataLoadRequest dataLoadRequest,
                                     List<CellLabel> labels,
                                     LabelPropagationMethod propagationMethod,
                                     LabelPropagationConfig propagationConfig,
                                     LogisticRegressionTrainingConfig trainingConfig,
                                     String modelNamePrefix,
                                     int samplingRound,
                                     StageEvaluator evaluator) {
        if (config == null || dataLoadRequest == null) {
            throw new IllegalArgumentException("任务配置和数据加载请求不能为空");
        }
        if (!config.getDatasetId().equals(dataLoadRequest.getDatasetId())) {
            throw new IllegalArgumentException("任务配置与加载请求的数据集标识不一致");
        }
        this.config = config;
        this.dataLoadRequest = dataLoadRequest;
        this.labels = labels == null ? Collections.<CellLabel>emptyList()
                : Collections.unmodifiableList(new ArrayList<CellLabel>(labels));
        this.propagationMethod = propagationMethod;
        this.propagationConfig = propagationConfig;
        this.trainingConfig = trainingConfig;
        this.modelNamePrefix = modelNamePrefix;
        this.samplingRound = samplingRound;
        this.evaluator = evaluator;
        validateByType();
    }

    public static RahaTaskExecutionRequest training(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            List<CellLabel> directLabels,
            LabelPropagationMethod propagationMethod,
            LabelPropagationConfig propagationConfig,
            LogisticRegressionTrainingConfig trainingConfig,
            String modelNamePrefix) {
        return training(config, dataLoadRequest, directLabels, propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix, null);
    }

    public static RahaTaskExecutionRequest training(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            List<CellLabel> directLabels,
            LabelPropagationMethod propagationMethod,
            LabelPropagationConfig propagationConfig,
            LogisticRegressionTrainingConfig trainingConfig,
            String modelNamePrefix,
            StageEvaluator evaluator) {
        return new RahaTaskExecutionRequest(config, dataLoadRequest, directLabels,
                propagationMethod, propagationConfig, trainingConfig,
                modelNamePrefix, 0, evaluator);
    }

    public static RahaTaskExecutionRequest detection(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest) {
        return detection(config, dataLoadRequest, null);
    }

    public static RahaTaskExecutionRequest detection(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            StageEvaluator evaluator) {
        return new RahaTaskExecutionRequest(config, dataLoadRequest,
                Collections.<CellLabel>emptyList(), null, null,
                null, null, 0, evaluator);
    }

    public static RahaTaskExecutionRequest sampling(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            List<CellLabel> existingLabels,
            int samplingRound) {
        return new RahaTaskExecutionRequest(config, dataLoadRequest, existingLabels,
                null, null, null, null, samplingRound, null);
    }

    private void validateByType() {
        JobType jobType = config.getJobType();
        if (jobType == JobType.TRAINING) {
            if (propagationMethod == null || propagationConfig == null
                    || trainingConfig == null) {
                throw new IllegalArgumentException("训练任务缺少传播或模型训练配置");
            }
            ValueUtils.requireNotBlank(modelNamePrefix, "模型名称前缀");
            for (CellLabel label : labels) {
                if (label == null || label.getLabelSource() == LabelSource.PROPAGATED) {
                    throw new IllegalArgumentException("训练任务只接受直接标签");
                }
            }
            return;
        }
        if (jobType == JobType.SAMPLING) {
            if (samplingRound <= 0) {
                throw new IllegalArgumentException("采样轮次必须大于零");
            }
            return;
        }
        if (jobType != JobType.DETECTION) {
            throw new IllegalArgumentException("统一入口暂不支持任务类型：" + jobType);
        }
    }

    public RahaJobConfig getConfig() { return config; }
    public DataLoadRequest getDataLoadRequest() { return dataLoadRequest; }
    public List<CellLabel> getLabels() { return labels; }
    public LabelPropagationMethod getPropagationMethod() { return propagationMethod; }
    public LabelPropagationConfig getPropagationConfig() { return propagationConfig; }
    public LogisticRegressionTrainingConfig getTrainingConfig() { return trainingConfig; }
    public String getModelNamePrefix() { return modelNamePrefix; }
    public int getSamplingRound() { return samplingRound; }
    public StageEvaluator getEvaluator() { return evaluator; }
}
