package com.fiberhome.ml.raha.audit;

import com.fiberhome.ml.raha.security.RahaResourceType;

import java.time.Clock;
import java.util.UUID;

/**
 * 统一生成并持久化审计事件，调用方只传递业务上下文和安全摘要。
 */
public final class RahaAuditService {

    /** 审计事件写入器。 */
    private final RahaAuditWriter writer;
    /** 提供可测试审计时间的时钟。 */
    private final Clock clock;

    public RahaAuditService(RahaAuditWriter writer, Clock clock) {
        if (writer == null || clock == null) {
            throw new IllegalArgumentException("审计服务依赖不能为空");
        }
        this.writer = writer;
        this.clock = clock;
    }

    public static RahaAuditService noOp(Clock clock) {
        return new RahaAuditService(NoOpRahaAuditWriter.getInstance(), clock);
    }

    /**
     * 记录一次可追溯操作，摘要不得包含输入原值。
     */
    public RahaAuditEvent record(String actor,
                                 RahaAuditAction action,
                                 RahaAuditStatus status,
                                 RahaResourceType resourceType,
                                 String resourceName,
                                 String datasetId,
                                 String jobId,
                                 String modelVersion,
                                 String summary) {
        RahaAuditEvent event = new RahaAuditEvent(UUID.randomUUID().toString(),
                actor, action, status, resourceType, resourceName, datasetId,
                jobId, modelVersion, summary, Math.max(1L, clock.millis()));
        writer.write(event);
        return event;
    }
}
