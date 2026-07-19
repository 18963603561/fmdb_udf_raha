package com.fiberhome.ml.raha.annotation.domain;

/**
 * 定义 Excel 标注导入可供调用方稳定处理的错误编码。
 */
public enum AnnotationImportErrorCode {
    /** 文件大小、行数或工作簿结构不符合约束。 */
    WORKBOOK_INVALID,
    /** 模板版本不受支持。 */
    TEMPLATE_VERSION_INVALID,
    /** 模板数据集、采样批次或月分区不匹配。 */
    SAMPLE_BATCH_MISMATCH,
    /** 模板字段模式与 c1 不一致。 */
    SCHEMA_HASH_MISMATCH,
    /** 系统行标识缺失。 */
    ROW_ID_MISSING,
    /** 标注任务标识不属于当前采样行。 */
    ANNOTATION_TASK_MISMATCH,
    /** 行标识不属于指定采样批次。 */
    ROW_NOT_IN_SAMPLE,
    /** 行内容哈希与 c1 不一致。 */
    ROW_CONTENT_HASH_MISMATCH,
    /** 受保护业务字段已被修改。 */
    BUSINESS_DATA_CHANGED,
    /** 整行标签不是零或一，或与异常字段语义冲突。 */
    ROW_LABEL_INVALID,
    /** 异常字段包含不存在或不可检测的字段。 */
    ERROR_COLUMN_INVALID,
    /** 文件内同一逻辑行出现多次。 */
    DUPLICATE_ROW,
    /** 相同文件已经导入。 */
    DUPLICATE_FILE,
    /** 修订批次不存在、跨数据集或形成非法引用。 */
    REVISION_INVALID
}
