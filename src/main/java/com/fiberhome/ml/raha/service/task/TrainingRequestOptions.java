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
}
