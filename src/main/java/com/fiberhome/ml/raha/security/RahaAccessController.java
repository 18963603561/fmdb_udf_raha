package com.fiberhome.ml.raha.security;

/**
 * 将权限适配器决策转换为统一拒绝异常，供业务主链路复用。
 */
public final class RahaAccessController {

    /** 实际权限适配器。 */
    private final RahaPermissionChecker permissionChecker;

    public RahaAccessController(RahaPermissionChecker permissionChecker) {
        if (permissionChecker == null) {
            throw new IllegalArgumentException("权限适配器不能为空");
        }
        this.permissionChecker = permissionChecker;
    }

    /**
     * 要求权限请求获得允许，否则抛出不包含敏感值的拒绝异常。
     */
    public RahaPermissionDecision requireAllowed(RahaPermissionRequest request) {
        RahaPermissionDecision decision = permissionChecker.check(request);
        if (decision == null) {
            throw new IllegalStateException("权限适配器返回空决策");
        }
        if (!decision.isAllowed()) {
            throw new RahaAccessDeniedException(request, decision);
        }
        return decision;
    }
}
