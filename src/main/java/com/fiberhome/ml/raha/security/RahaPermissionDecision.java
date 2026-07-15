package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存权限适配器的允许或拒绝结论及策略版本。
 */
public final class RahaPermissionDecision {

    /** 是否允许本次访问。 */
    private final boolean allowed;
    /** 不包含敏感值的决策原因。 */
    private final String reason;
    /** 产生决策的权限策略版本。 */
    private final String policyVersion;

    private RahaPermissionDecision(boolean allowed,
                                   String reason,
                                   String policyVersion) {
        this.allowed = allowed;
        this.reason = ValueUtils.requireNotBlank(reason, "权限决策原因");
        this.policyVersion = ValueUtils.requireNotBlank(
                policyVersion, "权限策略版本");
    }

    public static RahaPermissionDecision allow(String reason, String policyVersion) {
        return new RahaPermissionDecision(true, reason, policyVersion);
    }

    public static RahaPermissionDecision deny(String reason, String policyVersion) {
        return new RahaPermissionDecision(false, reason, policyVersion);
    }

    public boolean isAllowed() { return allowed; }
    public String getReason() { return reason; }
    public String getPolicyVersion() { return policyVersion; }
}
