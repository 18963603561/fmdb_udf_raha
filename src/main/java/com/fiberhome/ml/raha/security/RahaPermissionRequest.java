package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存一次权限校验所需的调用方、动作、资源和数据集上下文。
 */
public final class RahaPermissionRequest {

    /** 发起操作的调用方标识。 */
    private final String actor;
    /** 待校验动作。 */
    private final RahaPermissionAction action;
    /** 待访问资源类型。 */
    private final RahaResourceType resourceType;
    /** 不包含原始数据值的资源名称。 */
    private final String resourceName;
    /** 资源所属逻辑数据集。 */
    private final String datasetId;

    public RahaPermissionRequest(String actor,
                                 RahaPermissionAction action,
                                 RahaResourceType resourceType,
                                 String resourceName,
                                 String datasetId) {
        this.actor = ValueUtils.requireNotBlank(actor, "权限调用方");
        if (action == null || resourceType == null) {
            throw new IllegalArgumentException("权限动作和资源类型不能为空");
        }
        this.action = action;
        this.resourceType = resourceType;
        this.resourceName = ValueUtils.requireNotBlank(resourceName, "权限资源名称");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "权限数据集标识");
    }

    public String getActor() { return actor; }
    public RahaPermissionAction getAction() { return action; }
    public RahaResourceType getResourceType() { return resourceType; }
    public String getResourceName() { return resourceName; }
    public String getDatasetId() { return datasetId; }
}
