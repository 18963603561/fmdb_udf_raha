package com.fiberhome.ml.raha.audit;

import com.fiberhome.ml.raha.security.RahaResourceType;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 保存不包含原始单元格值的任务、模型、结果访问和清理审计事件。
 */
public final class RahaAuditEvent {

    /** 审计事件唯一标识。 */
    private final String eventId;
    /** 操作调用方。 */
    private final String actor;
    /** 审计动作。 */
    private final RahaAuditAction action;
    /** 操作状态。 */
    private final RahaAuditStatus status;
    /** 被操作资源类型。 */
    private final RahaResourceType resourceType;
    /** 不包含原始值的资源名称。 */
    private final String resourceName;
    /** 资源所属数据集。 */
    private final String datasetId;
    /** 可选任务标识。 */
    private final String jobId;
    /** 可选模型版本。 */
    private final String modelVersion;
    /** 不包含敏感值的操作摘要。 */
    private final String summary;
    /** 事件发生时间。 */
    private final long occurredAt;

    public RahaAuditEvent(String eventId,
                          String actor,
                          RahaAuditAction action,
                          RahaAuditStatus status,
                          RahaResourceType resourceType,
                          String resourceName,
                          String datasetId,
                          String jobId,
                          String modelVersion,
                          String summary,
                          long occurredAt) {
        this.eventId = ValueUtils.requireNotBlank(eventId, "审计事件标识");
        this.actor = ValueUtils.requireNotBlank(actor, "审计调用方");
        if (action == null || status == null || resourceType == null) {
            throw new IllegalArgumentException("审计动作、状态和资源类型不能为空");
        }
        if (occurredAt <= 0L) {
            throw new IllegalArgumentException("审计事件时间必须大于 0");
        }
        this.action = action;
        this.status = status;
        this.resourceType = resourceType;
        this.resourceName = ValueUtils.requireNotBlank(resourceName, "审计资源名称");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "审计数据集标识");
        this.jobId = blankToNull(jobId);
        this.modelVersion = blankToNull(modelVersion);
        this.summary = ValueUtils.requireNotBlank(summary, "审计摘要");
        this.occurredAt = occurredAt;
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value;
    }

    public String getEventId() { return eventId; }
    public String getActor() { return actor; }
    public RahaAuditAction getAction() { return action; }
    public RahaAuditStatus getStatus() { return status; }
    public RahaResourceType getResourceType() { return resourceType; }
    public String getResourceName() { return resourceName; }
    public String getDatasetId() { return datasetId; }
    public String getJobId() { return jobId; }
    public String getModelVersion() { return modelVersion; }
    public String getSummary() { return summary; }
    public long getOccurredAt() { return occurredAt; }
}
