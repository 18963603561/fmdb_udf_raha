package com.fiberhome.ml.raha.data.loader;

/**
 * 数据加载和行标识校验错误编码。
 */
public enum DataValidationErrorCode {
    /** 输入数据为空。 */
    EMPTY_DATASET,
    /** 行标识字段不存在。 */
    ROW_ID_COLUMN_MISSING,
    /** 行标识包含空值或空白值。 */
    ROW_ID_NULL_OR_BLANK,
    /** 行标识存在重复值。 */
    ROW_ID_DUPLICATED,
    /** 用户声明的单字段或联合键字段不存在。 */
    ROW_KEY_COLUMN_MISSING,
    /** 输入字段与 Raha 保留技术字段冲突。 */
    RESERVED_COLUMN_CONFLICT,
    /** 外部数据读取失败。 */
    DATA_LOAD_FAILED
}
