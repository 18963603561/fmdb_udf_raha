package com.fiberhome.ml.raha.annotation.domain;

/**
 * 定义标注文件导入和物理批次状态。
 */
public enum AnnotationBatchStatus {
    /** 文件全部标注行校验通过并入库。 */
    IMPORTED,
    /** 文件部分标注行校验通过，有效记录已入库。 */
    PARTIAL,
    /** 文件没有有效记录，不写标注业务表。 */
    REJECTED,
    /** 相同采样批次和文件内容已经成功导入。 */
    DUPLICATE
}
