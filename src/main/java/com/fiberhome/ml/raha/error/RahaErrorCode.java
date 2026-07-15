package com.fiberhome.ml.raha.error;

/**
 * 迭代 4 核心模块使用的稳定错误编码。
 */
public enum RahaErrorCode {
    /** 参数非法。 */
    PARAMETER_INVALID(RahaErrorCategory.PARAMETER),
    /** 输入数据与快照不一致。 */
    DATA_SNAPSHOT_CONFLICT(RahaErrorCategory.DATA),
    /** 策略执行失败。 */
    STRATEGY_EXECUTION_FAILED(RahaErrorCategory.STRATEGY),
    /** 特征输入与策略命中不一致。 */
    FEATURE_SNAPSHOT_CONFLICT(RahaErrorCategory.FEATURE),
    /** 检测模型不可用。 */
    DETECTION_MODEL_UNAVAILABLE(RahaErrorCategory.DETECTION),
    /** 结果存储失败。 */
    STORAGE_WRITE_FAILED(RahaErrorCategory.STORAGE),
    /** 未归类系统错误。 */
    SYSTEM_ERROR(RahaErrorCategory.SYSTEM);

    /** 错误所属类别。 */
    private final RahaErrorCategory category;

    RahaErrorCode(RahaErrorCategory category) {
        this.category = category;
    }

    public RahaErrorCategory getCategory() {
        return category;
    }
}
