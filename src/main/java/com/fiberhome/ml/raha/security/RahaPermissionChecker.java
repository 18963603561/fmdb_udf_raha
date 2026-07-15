package com.fiberhome.ml.raha.security;

/**
 * 隔离 FMDB、统一身份平台或本地规则引擎的权限校验实现。
 */
public interface RahaPermissionChecker {

    /**
     * 校验一次资源访问，适配器不得读取或记录原始单元格值。
     *
     * @param request 权限请求
     * @return 明确的允许或拒绝结论
     */
    RahaPermissionDecision check(RahaPermissionRequest request);
}
