package com.fiberhome.ml.raha.audit;

/**
 * 为既有开发入口提供的空审计实现，生产环境不得使用。
 */
public final class NoOpRahaAuditWriter implements RahaAuditWriter {

    /** 单例实例。 */
    private static final NoOpRahaAuditWriter INSTANCE = new NoOpRahaAuditWriter();

    private NoOpRahaAuditWriter() {
    }

    public static NoOpRahaAuditWriter getInstance() {
        return INSTANCE;
    }

    @Override
    public void write(RahaAuditEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("审计事件不能为空");
        }
    }
}
