package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.RowIdentityConfig;
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
    /** 可选 c1 采样批次引用。 */
    private final String sampleBatchId;
    /** c1 月分区。 */
    private final String samplePartitionMonth;
    /** 可选标注批次引用。 */
    private final String annotationBatchId;
    /** 标注月分区。 */
    private final String annotationPartitionMonth;
    /** c1 与 o1 必须共用的行身份配置。 */
    private final RowIdentityConfig rowIdentityConfig;
    /** 已在工作流画像前完成的训练合并结果。 */
    private final TrainingMergeResult trainingMergeResult;

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
                artifactVersion, null, null, null, null, null, null, null, null);
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
                artifactVersion, preparedFeatures, null, null, null, null, null, null,
                null);
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
        this(jobId, stageId, dataset, config, directLabels, propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix, artifactVersion,
                preparedFeatures, preparedPropagation, null, null, null, null, null,
                null);
    }

    /**
     * 使用工作流已经完成的合并、画像、特征和传播结果执行最终训练。
     */
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
                            LabelPropagationResult preparedPropagation,
                            TrainingMergeResult trainingMergeResult) {
        this(jobId, stageId, dataset, config, directLabels, propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix, artifactVersion,
                preparedFeatures, preparedPropagation, null, null, null, null, null,
                trainingMergeResult);
    }

    /**
     * 使用持久化 c1 和标注批次训练，训练服务会在算法开始前生成合并快照。
     */
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
                            String sampleBatchId,
                            String samplePartitionMonth,
                            String annotationBatchId,
                            String annotationPartitionMonth,
                            RowIdentityConfig rowIdentityConfig) {
        this(jobId, stageId, dataset, config, directLabels, propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix, artifactVersion,
                null, null, sampleBatchId, samplePartitionMonth,
                annotationBatchId, annotationPartitionMonth, rowIdentityConfig, null);
    }

    private RahaTrainRequest(String jobId,
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
                             LabelPropagationResult preparedPropagation,
                             String sampleBatchId,
                             String samplePartitionMonth,
                             String annotationBatchId,
                             String annotationPartitionMonth,
                             RowIdentityConfig rowIdentityConfig,
                             TrainingMergeResult trainingMergeResult) {
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
        boolean hasAnyBatchReference = sampleBatchId != null
                || samplePartitionMonth != null || annotationBatchId != null
                || annotationPartitionMonth != null || rowIdentityConfig != null;
        boolean hasAllBatchReferences = sampleBatchId != null
                && samplePartitionMonth != null && annotationBatchId != null
                && annotationPartitionMonth != null && rowIdentityConfig != null;
        if (hasAnyBatchReference && !hasAllBatchReferences) {
            throw new IllegalArgumentException("训练 c1、标注批次和行身份配置必须完整提供");
        }
        this.sampleBatchId = sampleBatchId;
        this.samplePartitionMonth = samplePartitionMonth;
        this.annotationBatchId = annotationBatchId;
        this.annotationPartitionMonth = annotationPartitionMonth;
        this.rowIdentityConfig = rowIdentityConfig;
        if (trainingMergeResult != null
                && (!dataset.getDatasetId().equals(
                trainingMergeResult.getDataset().getDatasetId())
                || !dataset.getSnapshotId().equals(
                trainingMergeResult.getTrainingSnapshotId())
                || trainingMergeResult.getDirectLabels().size()
                != this.directLabels.size())) {
            throw new IllegalArgumentException("训练合并结果与当前快照或直接标签不一致");
        }
        if (trainingMergeResult != null && hasAnyBatchReference) {
            throw new IllegalArgumentException("已合并训练请求不能再次携带批次引用");
        }
        this.trainingMergeResult = trainingMergeResult;
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
    public String getSampleBatchId() { return sampleBatchId; }
    public String getSamplePartitionMonth() { return samplePartitionMonth; }
    public String getAnnotationBatchId() { return annotationBatchId; }
    public String getAnnotationPartitionMonth() { return annotationPartitionMonth; }
    public RowIdentityConfig getRowIdentityConfig() { return rowIdentityConfig; }
    public TrainingMergeResult getTrainingMergeResult() { return trainingMergeResult; }

    public boolean hasPersistedInput() {
        return sampleBatchId != null;
    }

    /** 返回已完成 c1/o1 合并的请求，清除批次引用避免重复合并。 */
    public RahaTrainRequest withMergedInput(RahaDataset mergedDataset,
                                            List<CellLabel> mergedLabels) {
        if (mergedDataset == null || mergedLabels == null) {
            throw new IllegalArgumentException("合并训练数据和标签不能为空");
        }
        if (preparedFeatures != null || preparedPropagation != null) {
            throw new IllegalStateException("合并输入不能复用旧快照的训练产物");
        }
        return new RahaTrainRequest(jobId, stageId, mergedDataset, config,
                mergedLabels, propagationMethod, propagationConfig, trainingConfig,
                modelNamePrefix, artifactVersion, null, null, null, null, null,
                null, null, null);
    }

    /** 返回携带完整合并血缘的请求，供训练产物冻结使用。 */
    public RahaTrainRequest withMergedInput(TrainingMergeResult merged) {
        if (merged == null) {
            throw new IllegalArgumentException("训练合并结果不能为空");
        }
        if (preparedFeatures != null || preparedPropagation != null) {
            throw new IllegalStateException("合并输入不能复用旧快照的训练产物");
        }
        return new RahaTrainRequest(jobId, stageId, merged.getDataset(), config,
                merged.getDirectLabels(), propagationMethod, propagationConfig,
                trainingConfig, modelNamePrefix, artifactVersion, null, null,
                null, null, null, null, null, merged);
    }
}
