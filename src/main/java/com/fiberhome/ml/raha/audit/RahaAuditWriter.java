package com.fiberhome.ml.raha.audit;

/**
 * 隔离 FMDB 审计表和其他审计平台的持久化实现。
 */
public interface RahaAuditWriter {

    /**
     * 幂等保存一条审计事件。
     *
     * @param event 不包含原始单元格值的审计事件
     */
    void write(RahaAuditEvent event);
}
