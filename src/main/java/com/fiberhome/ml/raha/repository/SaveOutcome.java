package com.fiberhome.ml.raha.repository;

/**
 * 仓储保存结果，用于区分新增、版本更新和幂等重写。
 */
public enum SaveOutcome {
    /** 主键首次写入。 */
    CREATED,
    /** 主键相同但结果版本发生变化。 */
    UPDATED,
    /** 主键和结果版本均相同，本次写入被去重。 */
    UNCHANGED
}

