package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.util.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用调用方授权清单执行默认拒绝的权限校验，便于对接平台权限数据。
 */
public final class RuleBasedRahaPermissionChecker implements RahaPermissionChecker {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RuleBasedRahaPermissionChecker.class);
    /** 权限策略版本。 */
    private final String policyVersion;
    /** 按调用方保存的不可变授权规则。 */
    private final Map<String, List<RahaPermissionGrant>> grantsByActor;

    public RuleBasedRahaPermissionChecker(
            String policyVersion,
            Map<String, List<RahaPermissionGrant>> grantsByActor) {
        this.policyVersion = ValueUtils.requireNotBlank(
                policyVersion, "权限策略版本");
        if (grantsByActor == null) {
            throw new IllegalArgumentException("权限授权清单不能为空");
        }
        Map<String, List<RahaPermissionGrant>> copied =
                new LinkedHashMap<String, List<RahaPermissionGrant>>();
        for (Map.Entry<String, List<RahaPermissionGrant>> entry
                : grantsByActor.entrySet()) {
            String actor = ValueUtils.requireNotBlank(entry.getKey(), "授权调用方");
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("调用方授权规则不能为空");
            }
            copied.put(actor, Collections.unmodifiableList(
                    new ArrayList<RahaPermissionGrant>(entry.getValue())));
        }
        this.grantsByActor = Collections.unmodifiableMap(copied);
    }

    @Override
    public RahaPermissionDecision check(RahaPermissionRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("权限请求不能为空");
        }
        List<RahaPermissionGrant> grants = grantsByActor.get(request.getActor());
        if (grants != null) {
            for (RahaPermissionGrant grant : grants) {
                if (grant != null && grant.matches(request)) {
                    LOGGER.debug("权限校验通过，actor={}，action={}，resourceType={}，"
                                    + "resourceName={}，policyVersion={}",
                            request.getActor(), request.getAction(),
                            request.getResourceType(), request.getResourceName(),
                            policyVersion);
                    return RahaPermissionDecision.allow("命中授权规则", policyVersion);
                }
            }
        }
        LOGGER.warn("权限校验拒绝，actor={}，action={}，resourceType={}，"
                        + "resourceName={}，policyVersion={}",
                request.getActor(), request.getAction(), request.getResourceType(),
                request.getResourceName(), policyVersion);
        return RahaPermissionDecision.deny("未命中授权规则", policyVersion);
    }
}
