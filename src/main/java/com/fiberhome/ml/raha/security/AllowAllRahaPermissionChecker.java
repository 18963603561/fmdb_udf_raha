package com.fiberhome.ml.raha.security;

/**
 * 为兼容既有开发入口提供显式全放行适配器，生产环境不得使用。
 */
public final class AllowAllRahaPermissionChecker implements RahaPermissionChecker {

    /** 单例实例。 */
    private static final AllowAllRahaPermissionChecker INSTANCE =
            new AllowAllRahaPermissionChecker();

    private AllowAllRahaPermissionChecker() {
    }

    public static AllowAllRahaPermissionChecker getInstance() {
        return INSTANCE;
    }

    @Override
    public RahaPermissionDecision check(RahaPermissionRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("权限请求不能为空");
        }
        return RahaPermissionDecision.allow("开发兼容模式放行", "ALLOW_ALL_DEV");
    }
}
