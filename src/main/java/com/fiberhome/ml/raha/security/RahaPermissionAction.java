package com.fiberhome.ml.raha.security;

/**
 * 定义 Raha 生产资源支持的权限动作。
 */
public enum RahaPermissionAction {
    /** 提交异步任务。 */
    SUBMIT,
    /** 读取输入、模型或结果。 */
    READ,
    /** 写入任务产物或检测结果。 */
    WRITE,
    /** 发布模型。 */
    PUBLISH,
    /** 停用模型。 */
    DISABLE,
    /** 回滚模型。 */
    ROLLBACK,
    /** 清理过期数据。 */
    CLEANUP
}
