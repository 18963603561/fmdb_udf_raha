package com.fiberhome.ml.raha.production;

/**
 * 保存 P2 生产验收需要核实的工程和 FMDB 环境检查项。
 */
public final class ProductionReadinessContext {

    /** FMDB 读取、幂等写入和删除是否验证通过。 */
    private final boolean fmdbDataAccessReady;
    /** 三个表级 UDF 是否完成注册和调用验证。 */
    private final boolean udfReady;
    /** 生产权限适配器是否启用默认拒绝策略。 */
    private final boolean permissionReady;
    /** FMDB 审计写入是否可用。 */
    private final boolean auditReady;
    /** 敏感结果是否启用哈希或脱敏策略。 */
    private final boolean maskingReady;
    /** 过期中间结果清理是否完成演练。 */
    private final boolean retentionReady;
    /** 宽表、RVD 和并发限流是否通过。 */
    private final boolean wideTableReady;
    /** 大表和失败恢复是否通过。 */
    private final boolean recoveryReady;
    /** 是否已经形成目标集群性能基线。 */
    private final boolean clusterBaselineReady;

    public ProductionReadinessContext(boolean fmdbDataAccessReady,
                                      boolean udfReady,
                                      boolean permissionReady,
                                      boolean auditReady,
                                      boolean maskingReady,
                                      boolean retentionReady,
                                      boolean wideTableReady,
                                      boolean recoveryReady,
                                      boolean clusterBaselineReady) {
        this.fmdbDataAccessReady = fmdbDataAccessReady;
        this.udfReady = udfReady;
        this.permissionReady = permissionReady;
        this.auditReady = auditReady;
        this.maskingReady = maskingReady;
        this.retentionReady = retentionReady;
        this.wideTableReady = wideTableReady;
        this.recoveryReady = recoveryReady;
        this.clusterBaselineReady = clusterBaselineReady;
    }

    public boolean isFmdbDataAccessReady() { return fmdbDataAccessReady; }
    public boolean isUdfReady() { return udfReady; }
    public boolean isPermissionReady() { return permissionReady; }
    public boolean isAuditReady() { return auditReady; }
    public boolean isMaskingReady() { return maskingReady; }
    public boolean isRetentionReady() { return retentionReady; }
    public boolean isWideTableReady() { return wideTableReady; }
    public boolean isRecoveryReady() { return recoveryReady; }
    public boolean isClusterBaselineReady() { return clusterBaselineReady; }
}
