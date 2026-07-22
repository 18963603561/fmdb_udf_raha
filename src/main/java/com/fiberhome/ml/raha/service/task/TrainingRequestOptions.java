package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存持久化采样批次训练入口的标注选择和训练覆盖规则。
 */
public final class TrainingRequestOptions {

    /** 是否允许使用部分成功导入的标注批次。 */
    private final boolean allowPartialAnnotation;
    /** 候选模型名称前缀。 */
    private final String modelNamePrefix;
    /** 标签传播方法。 */
    private final LabelPropagationMethod propagationMethod;
    /** 可选受控输入覆盖，为空时从采样批次还原。 */
    private final FmdbInputSpec inputOverride;
    /** 执行覆盖选项，用于控制强制运行和请求级幂等。 */
    private final ExecutionOverrideOptions executionOverrideOptions;
    /** 请求显式指定的采样快照标识，用于检查点恢复训练。 */
    private final String requestedSnapshotId;
    /** 是否复用采样阶段固化的快照检查点。 */
    private final boolean reuseSnapshotCheckpoint;

    public TrainingRequestOptions(boolean allowPartialAnnotation,
                                  String modelNamePrefix,
                                  LabelPropagationMethod propagationMethod,
                                  FmdbInputSpec inputOverride) {
        this(allowPartialAnnotation, modelNamePrefix, propagationMethod,
                inputOverride, ExecutionOverrideOptions.DEFAULT);
    }

    public TrainingRequestOptions(boolean allowPartialAnnotation,
                                  String modelNamePrefix,
                                  LabelPropagationMethod propagationMethod,
                                  FmdbInputSpec inputOverride,
                                  ExecutionOverrideOptions executionOverrideOptions) {
        this(allowPartialAnnotation, modelNamePrefix, propagationMethod,
                inputOverride, executionOverrideOptions, null, false);
    }

    public TrainingRequestOptions(boolean allowPartialAnnotation,
                                  String modelNamePrefix,
                                  LabelPropagationMethod propagationMethod,
                                  FmdbInputSpec inputOverride,
                                  ExecutionOverrideOptions executionOverrideOptions,
                                  String requestedSnapshotId,
                                  boolean reuseSnapshotCheckpoint) {
        this.allowPartialAnnotation = allowPartialAnnotation;
        this.modelNamePrefix = ValueUtils.requireNotBlank(
                modelNamePrefix, "训练模型名称前缀");
        if (propagationMethod == null) {
            throw new IllegalArgumentException("训练标签传播方法不能为空");
        }
        this.propagationMethod = propagationMethod;
        this.inputOverride = inputOverride;
        this.executionOverrideOptions = executionOverrideOptions == null
                ? ExecutionOverrideOptions.DEFAULT : executionOverrideOptions;
        this.requestedSnapshotId = trimToNull(requestedSnapshotId);
        this.reuseSnapshotCheckpoint = reuseSnapshotCheckpoint;
    }

    /**
     * 创建只接受完整导入标注并使用同质性传播的默认选项。
     *
     * @return 默认训练选项
     */
    public static TrainingRequestOptions defaults() {
        return new TrainingRequestOptions(false, "raha",
                LabelPropagationMethod.HOMOGENEITY, null);
    }

    public boolean isAllowPartialAnnotation() { return allowPartialAnnotation; }
    public String getModelNamePrefix() { return modelNamePrefix; }
    public LabelPropagationMethod getPropagationMethod() { return propagationMethod; }
    public FmdbInputSpec getInputOverride() { return inputOverride; }
    public ExecutionOverrideOptions getExecutionOverrideOptions() {
        return executionOverrideOptions;
    }
    public String getRequestedSnapshotId() { return requestedSnapshotId; }
    public boolean isReuseSnapshotCheckpoint() { return reuseSnapshotCheckpoint; }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
