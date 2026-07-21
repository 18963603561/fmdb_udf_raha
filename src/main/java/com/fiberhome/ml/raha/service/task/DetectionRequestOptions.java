package com.fiberhome.ml.raha.service.task;

/**
 * 保存显式模型集合检测入口的非必填行为选项。
 */
public final class DetectionRequestOptions {

    /** 指定模型缺失或不兼容时的处理策略。 */
    private final MissingModelPolicy missingModelPolicy;
    /** 执行覆盖选项，用于控制强制运行和请求级幂等。 */
    private final ExecutionOverrideOptions executionOverrideOptions;

    public DetectionRequestOptions(MissingModelPolicy missingModelPolicy) {
        this(missingModelPolicy, ExecutionOverrideOptions.DEFAULT);
    }

    public DetectionRequestOptions(MissingModelPolicy missingModelPolicy,
                                   ExecutionOverrideOptions executionOverrideOptions) {
        if (missingModelPolicy == null) {
            throw new IllegalArgumentException("检测缺失模型策略不能为空");
        }
        this.missingModelPolicy = missingModelPolicy;
        this.executionOverrideOptions = executionOverrideOptions == null
                ? ExecutionOverrideOptions.DEFAULT : executionOverrideOptions;
    }

    /**
     * 创建任一字段缺少模型即失败的保守选项。
     *
     * @return 默认检测选项
     */
    public static DetectionRequestOptions defaults() {
        return new DetectionRequestOptions(MissingModelPolicy.FAIL);
    }

    public MissingModelPolicy getMissingModelPolicy() {
        return missingModelPolicy;
    }

    public ExecutionOverrideOptions getExecutionOverrideOptions() {
        return executionOverrideOptions;
    }
}
