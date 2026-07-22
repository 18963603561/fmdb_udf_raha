package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.job.stage.core.StageEvaluator;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.service.train.TrainingBatchReference;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchContext;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchOptions;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存统一任务入口执行训练、检测或采样所需的类型化输入。
 */
public final class RahaTaskExecutionRequest {

    /** 简单训练入口使用的默认模型名称前缀。 */
    private static final String DEFAULT_MODEL_NAME_PREFIX = "raha";

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
    /** 可选 c1 采样批次标识。 */
    private final String sampleBatchId;
    /** c1 采样月分区。 */
    private final String samplePartitionMonth;
    /** 可选标注批次标识。 */
    private final String annotationBatchId;
    /** 标注月分区。 */
    private final String annotationPartitionMonth;
    /** c1 与 o1 共用的行身份配置。 */
    private final RowIdentityConfig rowIdentityConfig;
    /** 一个或多个已经解析完成的持久化训练批次引用。 */
    private final List<TrainingBatchReference> trainingBatchReferences;
    /** 检测显式选择的不可变模型集合版本，旧入口使用当前已发布模型时为空。 */
    private final String modelSetVersion;
    /** 检测字段缺少模型或模型不兼容时的处理策略。 */
    private final MissingModelPolicy missingModelPolicy;
    /** 执行输入指纹元数据，区分基础血缘指纹和最终幂等指纹。 */
    private final ExecutionFingerprint executionFingerprint;
    /** 是否从采样快照检查点恢复训练前置产物。 */
    private final boolean reuseSnapshotCheckpoint;
    /** 父请求使用的列批执行参数。 */
    private final ColumnBatchOptions columnBatchOptions;
    /** 子请求所属列批上下文，父请求和普通请求为空。 */
    private final ColumnBatchContext columnBatchContext;
    /** 列批训练共享的模型集合版本。 */
    private final String modelSetVersionOverride;
    /** 列批训练共享的模型兼容计划版本。 */
    private final String modelCompatibilityVersionOverride;
    /** 列批检测共享的检测批次标识。 */
    private final String detectionBatchIdOverride;

    private RahaTaskExecutionRequest(RahaJobConfig config,
                                     DataLoadRequest dataLoadRequest,
                                     List<CellLabel> labels,
                                     LabelPropagationMethod propagationMethod,
                                     LabelPropagationConfig propagationConfig,
                                     LogisticRegressionTrainingConfig trainingConfig,
                                     String modelNamePrefix,
                                     int samplingRound,
                                     StageEvaluator evaluator,
                                     String sampleBatchId,
                                     String samplePartitionMonth,
                                     String annotationBatchId,
                                     String annotationPartitionMonth,
                                     RowIdentityConfig rowIdentityConfig) {
        this(config, dataLoadRequest, labels, propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix,
                samplingRound, evaluator, sampleBatchId, samplePartitionMonth,
                annotationBatchId, annotationPartitionMonth, rowIdentityConfig,
                legacyBatchReferences(sampleBatchId, samplePartitionMonth,
                        annotationBatchId, annotationPartitionMonth),
                null, MissingModelPolicy.PARTIAL, null, false,
                ColumnBatchOptions.disabled(), null, null, null, null);
    }

    private RahaTaskExecutionRequest(RahaJobConfig config,
                                     DataLoadRequest dataLoadRequest,
                                     List<CellLabel> labels,
                                     LabelPropagationMethod propagationMethod,
                                     LabelPropagationConfig propagationConfig,
                                     LogisticRegressionTrainingConfig trainingConfig,
                                     String modelNamePrefix,
                                     int samplingRound,
                                     StageEvaluator evaluator,
                                     String sampleBatchId,
                                     String samplePartitionMonth,
                                     String annotationBatchId,
                                     String annotationPartitionMonth,
                                     RowIdentityConfig rowIdentityConfig,
                                     List<TrainingBatchReference> trainingBatchReferences,
                                     String modelSetVersion,
                                     MissingModelPolicy missingModelPolicy,
                                     ExecutionFingerprint executionFingerprint,
                                     boolean reuseSnapshotCheckpoint,
                                     ColumnBatchOptions columnBatchOptions,
                                     ColumnBatchContext columnBatchContext,
                                     String modelSetVersionOverride,
                                     String modelCompatibilityVersionOverride,
                                     String detectionBatchIdOverride) {
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
        this.sampleBatchId = sampleBatchId;
        this.samplePartitionMonth = samplePartitionMonth;
        this.annotationBatchId = annotationBatchId;
        this.annotationPartitionMonth = annotationPartitionMonth;
        this.rowIdentityConfig = rowIdentityConfig;
        this.trainingBatchReferences = immutableBatchReferences(
                trainingBatchReferences);
        this.modelSetVersion = trimToNull(modelSetVersion);
        this.missingModelPolicy = missingModelPolicy == null
                ? MissingModelPolicy.FAIL : missingModelPolicy;
        this.executionFingerprint = executionFingerprint == null
                ? defaultExecutionFingerprint(config)
                : executionFingerprint;
        this.reuseSnapshotCheckpoint = reuseSnapshotCheckpoint;
        this.columnBatchOptions = columnBatchOptions == null
                ? ColumnBatchOptions.disabled() : columnBatchOptions;
        this.columnBatchContext = columnBatchContext;
        this.modelSetVersionOverride = trimToNull(modelSetVersionOverride);
        this.modelCompatibilityVersionOverride = trimToNull(
                modelCompatibilityVersionOverride);
        this.detectionBatchIdOverride = trimToNull(detectionBatchIdOverride);
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

    /**
     * 使用默认传播方式、传播配置、训练配置和模型名称前缀创建训练请求。
     *
     * @param config 完整任务配置
     * @param dataLoadRequest 数据加载请求
     * @param directLabels 调用方提供的直接标签
     * @return 使用默认训练参数的训练请求
     */
    public static RahaTaskExecutionRequest training(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            List<CellLabel> directLabels) {
        return training(config, dataLoadRequest, directLabels,
                LabelPropagationMethod.HOMOGENEITY,
                LabelPropagationConfig.defaults(),
                LogisticRegressionTrainingConfig.defaults(),
                DEFAULT_MODEL_NAME_PREFIX);
    }

    /**
     * 使用调用方训练配置创建简化训练请求。
     *
     * 该重载保留配置对象的显式控制，同时默认使用保守的同质性传播方式和
     * 固定模型名称前缀，适合不需要评估器和高级批次参数的常规训练任务。
     *
     * @param config 完整任务配置
     * @param dataLoadRequest 数据加载请求
     * @param trainingConfig 列模型训练配置
     * @param propagationConfig 标签传播配置
     * @param directLabels 调用方提供的直接标签
     * @return 简化训练请求
     */
    public static RahaTaskExecutionRequest training(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            LogisticRegressionTrainingConfig trainingConfig,
            LabelPropagationConfig propagationConfig,
            List<CellLabel> directLabels) {
        return training(config, dataLoadRequest, directLabels,
                LabelPropagationMethod.HOMOGENEITY,
                propagationConfig, trainingConfig,
                DEFAULT_MODEL_NAME_PREFIX);
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
                modelNamePrefix, 0, evaluator, null, null, null, null, null);
    }

    /**
     * 创建使用持久化 c1 和标注批次的训练任务，标签由合并阶段从标注仓储读取。
     */
    /**
     * 创建从采样快照检查点恢复前置产物的训练请求。
     */
    public static RahaTaskExecutionRequest trainingFromSnapshotCheckpoint(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            List<CellLabel> directLabels,
            LabelPropagationMethod propagationMethod,
            LabelPropagationConfig propagationConfig,
            LogisticRegressionTrainingConfig trainingConfig,
            String modelNamePrefix) {
        return new RahaTaskExecutionRequest(config, dataLoadRequest, directLabels,
                propagationMethod, propagationConfig, trainingConfig,
                modelNamePrefix, 0, null, null, null, null, null, null,
                Collections.<TrainingBatchReference>emptyList(), null,
                MissingModelPolicy.FAIL, null, true,
                ColumnBatchOptions.disabled(), null, null, null, null);
    }

    public static RahaTaskExecutionRequest training(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            LabelPropagationMethod propagationMethod,
            LabelPropagationConfig propagationConfig,
            LogisticRegressionTrainingConfig trainingConfig,
            String modelNamePrefix,
            String sampleBatchId,
            String samplePartitionMonth,
            String annotationBatchId,
            String annotationPartitionMonth,
            RowIdentityConfig rowIdentityConfig) {
        return new RahaTaskExecutionRequest(config, dataLoadRequest,
                Collections.<CellLabel>emptyList(), propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix, 0, null,
                sampleBatchId, samplePartitionMonth, annotationBatchId,
                annotationPartitionMonth, rowIdentityConfig);
    }

    /**
     * 创建已经解析好一个或多个持久化采样和标注批次的训练请求。
     *
     * @param config 完整训练任务配置
     * @param dataLoadRequest 当前原始输入加载请求
     * @param propagationMethod 标签传播方法
     * @param propagationConfig 标签传播配置
     * @param trainingConfig 模型训练配置
     * @param modelNamePrefix 模型名称前缀
     * @param batchReferences 按稳定顺序保存的训练批次引用
     * @param rowIdentityConfig 全部批次共用的行身份规则
     * @return 多批次持久化训练请求
     */
    public static RahaTaskExecutionRequest trainingBatches(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            LabelPropagationMethod propagationMethod,
            LabelPropagationConfig propagationConfig,
            LogisticRegressionTrainingConfig trainingConfig,
            String modelNamePrefix,
            List<TrainingBatchReference> batchReferences,
            RowIdentityConfig rowIdentityConfig) {
        List<TrainingBatchReference> references = immutableBatchReferences(
                batchReferences);
        if (references.isEmpty()) {
            throw new IllegalArgumentException("持久化训练批次引用不能为空");
        }
        TrainingBatchReference first = references.get(0);
        return new RahaTaskExecutionRequest(config, dataLoadRequest,
                Collections.<CellLabel>emptyList(), propagationMethod,
                propagationConfig, trainingConfig, modelNamePrefix, 0, null,
                first.getSampleBatchId(), first.getSamplePartitionMonth(),
                first.getAnnotationBatchId(),
                first.getAnnotationPartitionMonth(), rowIdentityConfig,
                references, null, MissingModelPolicy.FAIL, null, false,
                ColumnBatchOptions.disabled(), null, null, null, null);
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
                null, null, 0, evaluator, null, null, null, null, null);
    }

    /**
     * 创建显式选择不可变模型集合的检测请求。
     *
     * @param config 完整检测任务配置，必须包含模型集合输入指纹
     * @param dataLoadRequest 待检测数据加载请求
     * @param modelSetVersion 不可变模型集合版本
     * @param missingModelPolicy 缺少字段模型时的处理策略
     * @return 显式模型集合检测请求
     */
    public static RahaTaskExecutionRequest detection(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            String modelSetVersion,
            MissingModelPolicy missingModelPolicy) {
        return new RahaTaskExecutionRequest(config, dataLoadRequest,
                Collections.<CellLabel>emptyList(), null, null, null, null,
                0, null, null, null, null, null, null,
                Collections.<TrainingBatchReference>emptyList(),
                modelSetVersion, missingModelPolicy, null, false,
                ColumnBatchOptions.disabled(), null, null, null, null);
    }

    public static RahaTaskExecutionRequest sampling(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest,
            List<CellLabel> existingLabels,
            int samplingRound) {
        return new RahaTaskExecutionRequest(config, dataLoadRequest, existingLabels,
                null, null, null, null, samplingRound, null,
                null, null, null, null, null);
    }

    /**
     * 使用空标签集合和第 1 轮创建简化采样请求。
     *
     * @param config 完整任务配置
     * @param dataLoadRequest 数据加载请求
     * @return 第 1 轮采样请求
     */
    public static RahaTaskExecutionRequest sampling(
            RahaJobConfig config,
            DataLoadRequest dataLoadRequest) {
        return sampling(config, dataLoadRequest,
                Collections.<CellLabel>emptyList(), 1);
    }

    private void validateByType() {
        JobType jobType = config.getJobType();
        boolean hasAnyBatchReference = sampleBatchId != null
                || samplePartitionMonth != null || annotationBatchId != null
                || annotationPartitionMonth != null || rowIdentityConfig != null;
        boolean hasAllBatchReferences = sampleBatchId != null
                && samplePartitionMonth != null && annotationBatchId != null
                && annotationPartitionMonth != null && rowIdentityConfig != null;
        if (!trainingBatchReferences.isEmpty() && rowIdentityConfig == null) {
            throw new IllegalArgumentException("多批次训练必须提供统一行身份配置");
        }
        if (hasAnyBatchReference && !hasAllBatchReferences) {
            throw new IllegalArgumentException("训练批次引用和行身份配置必须完整提供");
        }
        if (jobType == JobType.TRAINING) {
            if (reuseSnapshotCheckpoint
                    && (config.getSnapshotId() == null
                    || config.getSnapshotId().trim().isEmpty())) {
                throw new IllegalArgumentException(
                        "检查点复用训练必须指定 snapshotId");
            }
            if (modelSetVersion != null) {
                throw new IllegalArgumentException("训练任务不能指定检测模型集合");
            }
            if (detectionBatchIdOverride != null) {
                throw new IllegalArgumentException("训练任务不能指定检测批次覆盖");
            }
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
            if (hasAllBatchReferences && !labels.isEmpty()) {
                throw new IllegalArgumentException("持久化标注训练不能同时传入调用方标签");
            }
            return;
        }
        if (reuseSnapshotCheckpoint) {
            throw new IllegalArgumentException("仅训练任务可以复用快照检查点");
        }
        if (hasAnyBatchReference) {
            throw new IllegalArgumentException("仅训练任务可以引用 c1 和标注批次");
        }
        if (jobType == JobType.SAMPLING) {
            if (modelSetVersion != null || modelSetVersionOverride != null
                    || detectionBatchIdOverride != null) {
                throw new IllegalArgumentException("采样任务不能指定检测模型集合");
            }
            if (samplingRound <= 0) {
                throw new IllegalArgumentException("采样轮次必须大于零");
            }
            return;
        }
        if (jobType != JobType.DETECTION) {
            throw new IllegalArgumentException("统一入口暂不支持任务类型：" + jobType);
        }
        if (modelSetVersion != null
                && config.getExecutionInputFingerprint() == null) {
            throw new IllegalArgumentException("显式模型集合必须进入任务执行输入指纹");
        }
        if (modelSetVersionOverride != null
                || modelCompatibilityVersionOverride != null) {
            throw new IllegalArgumentException("检测任务不能指定训练模型版本覆盖");
        }
        if (columnBatchContext != null && !columnBatchOptions.isEnabled()
                && detectionBatchIdOverride == null) {
            throw new IllegalArgumentException("检测列批子任务必须指定父检测批次");
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
    public String getSampleBatchId() { return sampleBatchId; }
    public String getSamplePartitionMonth() { return samplePartitionMonth; }
    public String getAnnotationBatchId() { return annotationBatchId; }
    public String getAnnotationPartitionMonth() { return annotationPartitionMonth; }
    public RowIdentityConfig getRowIdentityConfig() { return rowIdentityConfig; }
    public List<TrainingBatchReference> getTrainingBatchReferences() {
        return trainingBatchReferences;
    }
    public String getModelSetVersion() { return modelSetVersion; }
    public MissingModelPolicy getMissingModelPolicy() { return missingModelPolicy; }
    public ExecutionFingerprint getExecutionFingerprint() {
        return executionFingerprint;
    }
    public String getBaseExecutionInputFingerprint() {
        return executionFingerprint.getBaseExecutionInputFingerprint();
    }
    public String getExecutionInputFingerprint() {
        return executionFingerprint.getExecutionInputFingerprint();
    }
    public boolean isForceRun() { return executionFingerprint.isForceRun(); }
    public String getForceRunId() { return executionFingerprint.getForceRunId(); }
    public String getRunNonce() { return executionFingerprint.getRunNonce(); }
    public boolean isReuseSnapshotCheckpoint() { return reuseSnapshotCheckpoint; }
    public ColumnBatchOptions getColumnBatchOptions() { return columnBatchOptions; }
    public ColumnBatchContext getColumnBatchContext() { return columnBatchContext; }
    public String getModelSetVersionOverride() { return modelSetVersionOverride; }
    public String getModelCompatibilityVersionOverride() {
        return modelCompatibilityVersionOverride;
    }
    public String getDetectionBatchIdOverride() { return detectionBatchIdOverride; }
    public boolean isColumnBatchChild() { return columnBatchContext != null; }

    /**
     * 创建包含请求指纹元数据的新请求副本。
     *
     * @param fingerprint 请求工厂已生成的指纹结果
     * @return 保留业务输入的新请求
     */
    public RahaTaskExecutionRequest withExecutionFingerprint(
            ExecutionFingerprint fingerprint) {
        if (fingerprint == null) {
            throw new IllegalArgumentException("执行输入指纹元数据不能为空");
        }
        return new RahaTaskExecutionRequest(config, dataLoadRequest, labels,
                propagationMethod, propagationConfig, trainingConfig,
                modelNamePrefix, samplingRound, evaluator, sampleBatchId,
                samplePartitionMonth, annotationBatchId,
                annotationPartitionMonth, rowIdentityConfig,
                trainingBatchReferences, modelSetVersion, missingModelPolicy,
                fingerprint, reuseSnapshotCheckpoint, columnBatchOptions,
                columnBatchContext, modelSetVersionOverride,
                modelCompatibilityVersionOverride, detectionBatchIdOverride);
    }

    /**
     * 创建携带父级列批参数的新请求副本。
     *
     * @param options 列批执行参数
     * @return 保留业务输入的新父请求
     */
    public RahaTaskExecutionRequest withColumnBatchOptions(
            ColumnBatchOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("列批执行参数不能为空");
        }
        return new RahaTaskExecutionRequest(config, dataLoadRequest, labels,
                propagationMethod, propagationConfig, trainingConfig,
                modelNamePrefix, samplingRound, evaluator, sampleBatchId,
                samplePartitionMonth, annotationBatchId,
                annotationPartitionMonth, rowIdentityConfig,
                trainingBatchReferences, modelSetVersion, missingModelPolicy,
                executionFingerprint, reuseSnapshotCheckpoint, options,
                columnBatchContext, modelSetVersionOverride,
                modelCompatibilityVersionOverride, detectionBatchIdOverride);
    }

    /**
     * 创建限制到单个字段批次的子任务请求。
     *
     * @param childConfig 已写入子任务指纹和策略字段范围的配置
     * @param childLoadRequest 已写入字段白名单的数据加载请求
     * @param context 父子列批上下文
     * @param trainingModelSetVersion 训练共享模型集合版本
     * @param modelCompatibilityVersion 训练共享模型兼容计划版本
     * @param detectionBatchId 检测共享父批次标识
     * @return 禁止再次递归拆批的子任务请求
     */
    public RahaTaskExecutionRequest toColumnBatchChild(
            RahaJobConfig childConfig,
            DataLoadRequest childLoadRequest,
            ColumnBatchContext context,
            String trainingModelSetVersion,
            String modelCompatibilityVersion,
            String detectionBatchId) {
        if (context == null) {
            throw new IllegalArgumentException("列批子任务上下文不能为空");
        }
        ExecutionFingerprint childFingerprint = ExecutionFingerprint
                .fromConfig(childConfig.getExecutionInputFingerprint());
        return new RahaTaskExecutionRequest(childConfig, childLoadRequest,
                labels, propagationMethod, propagationConfig, trainingConfig,
                modelNamePrefix, samplingRound, evaluator, sampleBatchId,
                samplePartitionMonth, annotationBatchId,
                annotationPartitionMonth, rowIdentityConfig,
                trainingBatchReferences, modelSetVersion, missingModelPolicy,
                childFingerprint, false, ColumnBatchOptions.disabled(),
                context, trainingModelSetVersion, modelCompatibilityVersion,
                detectionBatchId);
    }

    /**
     * 创建可进入任务结果摘要的请求级元数据。
     *
     * @return 包含指纹、强制运行参数和任务输入的有序摘要
     */
    public Map<String, Object> executionSummarySeed() {
        Map<String, Object> result = new LinkedHashMap<String, Object>(
                executionFingerprint.toSummaryMap());
        result.put("jobType", config.getJobType().name());
        result.put("datasetId", config.getDatasetId());
        result.put("inputReference", config.getInputReference());
        result.put("snapshotId", config.getSnapshotId());
        result.put("reuseSnapshotCheckpoint",
                Boolean.valueOf(reuseSnapshotCheckpoint));
        result.put("columnBatchSize",
                Integer.valueOf(columnBatchOptions.getColumnBatchSize()));
        result.put("batchRvdEnabled",
                Boolean.valueOf(columnBatchOptions.isBatchRvdEnabled()));
        if (columnBatchContext != null) {
            result.put("parentJobId", columnBatchContext.getParentJobId());
            result.put("columnBatchId", columnBatchContext.getBatchId());
            result.put("columnBatchIndex",
                    Integer.valueOf(columnBatchContext.getBatchIndex()));
            result.put("columnBatchColumns", columnBatchContext.getColumns());
        }
        if (modelSetVersionOverride != null) {
            result.put("modelSetVersionOverride", modelSetVersionOverride);
        }
        if (detectionBatchIdOverride != null) {
            result.put("detectionBatchIdOverride", detectionBatchIdOverride);
        }
        if (modelSetVersion != null) {
            result.put("modelSetVersion", modelSetVersion);
        }
        result.put("missingModelPolicy", missingModelPolicy.name());
        if (sampleBatchId != null) {
            result.put("sampleBatchId", sampleBatchId);
        }
        if (annotationBatchId != null) {
            result.put("annotationBatchId", annotationBatchId);
        }
        result.put("samplingRound", Integer.valueOf(samplingRound));
        return result;
    }

    public boolean hasPersistedTrainingInput() {
        return !trainingBatchReferences.isEmpty();
    }

    private static List<TrainingBatchReference> legacyBatchReferences(
            String sampleBatchId,
            String samplePartitionMonth,
            String annotationBatchId,
            String annotationPartitionMonth) {
        if (sampleBatchId == null || samplePartitionMonth == null
                || annotationBatchId == null || annotationPartitionMonth == null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(new TrainingBatchReference(
                sampleBatchId, samplePartitionMonth, annotationBatchId,
                annotationPartitionMonth));
    }

    private static List<TrainingBatchReference> immutableBatchReferences(
            List<TrainingBatchReference> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<TrainingBatchReference> result =
                new ArrayList<TrainingBatchReference>(values.size());
        for (TrainingBatchReference value : values) {
            if (value == null) {
                throw new IllegalArgumentException("训练批次引用不能包含空值");
            }
            result.add(value);
        }
        return Collections.unmodifiableList(result);
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static ExecutionFingerprint defaultExecutionFingerprint(
            RahaJobConfig config) {
        String fingerprint = trimToNull(config.getExecutionInputFingerprint());
        if (fingerprint == null) {
            // 老入口可能没有显式执行输入指纹，兜底指纹仅用于观测，不写回配置。
            fingerprint = HashUtils.md5Hex(config.toCanonicalString());
        }
        return ExecutionFingerprint.fromConfig(fingerprint);
    }
}
