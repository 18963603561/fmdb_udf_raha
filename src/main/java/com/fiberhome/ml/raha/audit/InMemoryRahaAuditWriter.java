package com.fiberhome.ml.raha.audit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 开发和测试使用的幂等内存审计写入器。
 */
public final class InMemoryRahaAuditWriter implements RahaAuditWriter {

    /** 按事件标识保存的审计事件。 */
    private final Map<String, RahaAuditEvent> events =
            new LinkedHashMap<String, RahaAuditEvent>();

    @Override
    public synchronized void write(RahaAuditEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("审计事件不能为空");
        }
        events.put(event.getEventId(), event);
    }

    public synchronized List<RahaAuditEvent> findAll() {
        return Collections.unmodifiableList(
                new ArrayList<RahaAuditEvent>(events.values()));
    }
}
