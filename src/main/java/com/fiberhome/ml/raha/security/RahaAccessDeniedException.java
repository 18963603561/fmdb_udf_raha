package com.fiberhome.ml.raha.security;

/**
 * 表示调用方没有指定资源动作权限。
 */
public final class RahaAccessDeniedException extends RuntimeException {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 被拒绝权限请求。 */
    private final RahaPermissionRequest request;
    /** 权限适配器拒绝决策。 */
    private final RahaPermissionDecision decision;

    public RahaAccessDeniedException(RahaPermissionRequest request,
                                     RahaPermissionDecision decision) {
        super("调用方没有资源访问权限");
        if (request == null || decision == null || decision.isAllowed()) {
            throw new IllegalArgumentException("权限拒绝异常必须包含有效拒绝上下文");
        }
        this.request = request;
        this.decision = decision;
    }

    public RahaPermissionRequest getRequest() { return request; }
    public RahaPermissionDecision getDecision() { return decision; }
}
