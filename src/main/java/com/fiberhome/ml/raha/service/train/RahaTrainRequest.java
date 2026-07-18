package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationResult;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.service.prepare.RahaFeaturePreparationResult;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存一次 Raha 列级模型训练服务的全部输入。
 */
public final class RahaTrainRequest {

    /** 训练任务标识。 */
    private final String jobId;
    /** 训练服务阶段标识。 */
    private final String stageId;
    /** 已加载并完成画像的数据集。 */
    private final RahaDataset dataset;
    /** 训练任务完整配置。 */
    private final RahaJobConfig config;
    /** 人工、真值或规则确认的直接标签。 */
    private final List<CellLabel> directLabels;
    /** 标签传播方式。 */
    private final LabelPropagationMethod propagationMethod;
    /** 标签传播权重和多数比例配置。 */
    private final LabelPropagationConfig propagationConfig;
    /** 逻辑回归优化配置。 */
    private final LogisticRegressionTrainingConfig trainingConfig;
    /** 模型逻辑名称前缀。 */
    private final String modelNamePrefix;
    /** 训练服务仓储业务版本。 */
    private final ArtifactVersion artifactVersion;
    /** SAMPLE 阶段已经生成的可复用策略和特征产物。 */
    private final RahaFeaturePreparationResult preparedFeatures;
    /** PROPAGATE 阶段已经生成的可复用标签传播结果。 */
    private final LabelPropagationResult preparedPropagation;

    public RahaTrainRequest(String jobId,
                            String stageId,
                            RahaDataset dataset,
                            RahaJobConfig config,
                            List<CellLabel> directLabels,
                            LabelPropagationMethod propagationMethod,
                            LabelPropagationConfig propagationConfig,
                            LogisticRegressionTrainingConfig trainingConfig,
                            String modelNamePrefix,
                            ArtifactVersion artifactVersion) {
        this(jobId, stageId, dataset, config, directLabels, propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix,
                artifactVersion, null, null);
    }

    public RahaTrainRequest(String jobId,
                            String stageId,
                            RahaDataset dataset,
                            RahaJobConfig config,
                            List<CellLabel> directLabels,
                            LabelPropagationMethod propagationMethod,
                            LabelPropagationConfig propagationConfig,
                            LogisticRegressionTrainingConfig trainingConfig,
                            String modelNamePrefix,
                            ArtifactVersion artifactVersion,
                            RahaFeaturePreparationResult preparedFeatures) {
        this(jobId, stageId, dataset, config, directLabels, propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix,
                artifactVersion, preparedFeatures, null);
    }

    public RahaTrainRequest(String jobId,
                            String stageId,
                            RahaDataset dataset,
                            RahaJobConfig config,
                            List<CellLabel> directLabels,
                            LabelPropagationMethod propagationMethod,
                            LabelPropagationConfig propagationConfig,
                            LogisticRegressionTrainingConfig trainingConfig,
                            String modelNamePrefix,
                            ArtifactVersion artifactVersion,
                            RahaFeaturePreparationResult preparedFeatures,
                            LabelPropagationResult preparedPropagation) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "训练任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "训练阶段标识");
        this.modelNamePrefix = ValueUtils.requireNotBlank(modelNamePrefix, "模型名称前缀");
        if (dataset == null || dataset.getDataFrame() == null || config == null
                || directLabels == null || propagationMethod == null
                || propagationConfig == null || trainingConfig == null
                || artifactVersion == null) {
            throw new IllegalArgumentException("训练服务输入、配置和版本不能为空");
        }
        if (!dataset.getDatasetId().equals(config.getDatasetId())) {
            throw new IllegalArgumentException("训练数据集与任务配置标识不一致");
        }
        for (CellLabel label : directLabels) {
            if (label == null || label.getLabelSource() == LabelSource.PROPAGATED) {
                throw new IllegalArgumentException("训练服务只接受直接标签作为传播输入");
            }
        }
        this.dataset = dataset;
        this.config = config;
        this.directLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(directLabels));
        this.propagationMethod = propagationMethod;
        this.propagationConfig = propagationConfig;
        this.trainingConfig = trainingConfig;
        this.artifactVersion = artifactVersion;
        if (preparedFeatures != null
                && (!dataset.getDatasetId().equals(preparedFeatures.getDatasetId())
                || !dataset.getSnapshotId().equals(preparedFeatures.getSnapshotId()))) {
            throw new IllegalArgumentException("复用特征产物与训练数据集快照不一致");
        }
        this.preparedFeatures = preparedFeatures;
        if (preparedPropagation != null
                && preparedPropagation.getMetrics().getDirectLabelCount()
                != this.directLabels.size()) {
            throw new IllegalArgumentException("复用传播结果与训练直接标签数量不一致");
        }
        this.preparedPropagation = preparedPropagation;
    }

    public String getJobId() { return jobId; }
    public String getStageId() { return stageId; }
    public RahaDataset getDataset() { return dataset; }
    public RahaJobConfig getConfig() { return config; }
    public List<CellLabel> getDirectLabels() { return directLabels; }
    public LabelPropagationMethod getPropagationMethod() { return propagationMethod; }
    public LabelPropagationConfig getPropagationConfig() { return propagationConfig; }
    public LogisticRegressionTrainingConfig getTrainingConfig() { return trainingConfig; }
    public String getModelNamePrefix() { return modelNamePrefix; }
    public ArtifactVersion getArtifactVersion() { return artifactVersion; }
    public RahaFeaturePreparationResult getPreparedFeatures() { return preparedFeatures; }
    public LabelPropagationResult getPreparedPropagation() { return preparedPropagation; }
}
