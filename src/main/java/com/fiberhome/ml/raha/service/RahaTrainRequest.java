package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.config.RahaJobConfig;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.LabelPropagationMethod;
import com.fiberhome.ml.raha.model.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.model.TreeModelTrainingConfig;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
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
    /** 决策树和梯度提升树训练配置。 */
    private final TreeModelTrainingConfig treeTrainingConfig;
    /** 模型逻辑名称前缀。 */
    private final String modelNamePrefix;
    /** 训练服务仓储业务版本。 */
    private final ArtifactVersion artifactVersion;

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
                propagationConfig, trainingConfig, TreeModelTrainingConfig.defaults(),
                modelNamePrefix, artifactVersion);
    }

    public RahaTrainRequest(String jobId,
                            String stageId,
                            RahaDataset dataset,
                            RahaJobConfig config,
                            List<CellLabel> directLabels,
                            LabelPropagationMethod propagationMethod,
                            LabelPropagationConfig propagationConfig,
                            LogisticRegressionTrainingConfig trainingConfig,
                            TreeModelTrainingConfig treeTrainingConfig,
                            String modelNamePrefix,
                            ArtifactVersion artifactVersion) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "训练任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "训练阶段标识");
        this.modelNamePrefix = ValueUtils.requireNotBlank(modelNamePrefix, "模型名称前缀");
        if (dataset == null || dataset.getDataFrame() == null || config == null
                || directLabels == null || propagationMethod == null
                || propagationConfig == null || trainingConfig == null
                || treeTrainingConfig == null
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
        this.treeTrainingConfig = treeTrainingConfig;
        this.artifactVersion = artifactVersion;
    }

    public String getJobId() { return jobId; }
    public String getStageId() { return stageId; }
    public RahaDataset getDataset() { return dataset; }
    public RahaJobConfig getConfig() { return config; }
    public List<CellLabel> getDirectLabels() { return directLabels; }
    public LabelPropagationMethod getPropagationMethod() { return propagationMethod; }
    public LabelPropagationConfig getPropagationConfig() { return propagationConfig; }
    public LogisticRegressionTrainingConfig getTrainingConfig() { return trainingConfig; }
    public TreeModelTrainingConfig getTreeTrainingConfig() { return treeTrainingConfig; }
    public String getModelNamePrefix() { return modelNamePrefix; }
    public ArtifactVersion getArtifactVersion() { return artifactVersion; }
}
