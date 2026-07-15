package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 表示授予一个调用方的动作、资源类型和资源名称规则。
 */
public final class RahaPermissionGrant {

    /** 通配全部资源名称或数据集的固定标记。 */
    public static final String WILDCARD = "*";
    /** 被授权动作。 */
    private final RahaPermissionAction action;
    /** 被授权资源类型。 */
    private final RahaResourceType resourceType;
    /** 精确资源名称或通配标记。 */
    private final String resourceName;
    /** 精确数据集标识或通配标记。 */
    private final String datasetId;

    public RahaPermissionGrant(RahaPermissionAction action,
                               RahaResourceType resourceType,
                               String resourceName,
                               String datasetId) {
        if (action == null || resourceType == null) {
            throw new IllegalArgumentException("授权动作和资源类型不能为空");
        }
        this.action = action;
        this.resourceType = resourceType;
        this.resourceName = ValueUtils.requireNotBlank(resourceName, "授权资源名称");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "授权数据集标识");
    }

    /**
     * 判断授权规则是否覆盖指定权限请求。
     */
    public boolean matches(RahaPermissionRequest request) {
        return request != null
                && action == request.getAction()
                && resourceType == request.getResourceType()
                && matchesValue(resourceName, request.getResourceName())
                && matchesValue(datasetId, request.getDatasetId());
    }

    private static boolean matchesValue(String rule, String actual) {
        return WILDCARD.equals(rule) || rule.equals(actual);
    }
}
