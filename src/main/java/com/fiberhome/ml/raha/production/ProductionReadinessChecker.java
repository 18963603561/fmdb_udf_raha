package com.fiberhome.ml.raha.production;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 按固定检查项生成 P2 生产就绪结论，防止遗漏真实集群性能基线。
 */
public final class ProductionReadinessChecker {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ProductionReadinessChecker.class);

    public ProductionReadinessReport check(ProductionReadinessContext context) {
        if (context == null) {
            throw new IllegalArgumentException("生产验收上下文不能为空");
        }
        Map<String, Boolean> checks = new LinkedHashMap<String, Boolean>();
        checks.put("fmdbDataAccess", context.isFmdbDataAccessReady());
        checks.put("udfRegistration", context.isUdfReady());
        checks.put("permission", context.isPermissionReady());
        checks.put("audit", context.isAuditReady());
        checks.put("masking", context.isMaskingReady());
        checks.put("retention", context.isRetentionReady());
        checks.put("wideTableAndRvd", context.isWideTableReady());
        checks.put("largeTableRecovery", context.isRecoveryReady());
        checks.put("targetClusterBaseline", context.isClusterBaselineReady());
        ProductionReadinessReport report = new ProductionReadinessReport(checks);
        if (report.isReady()) {
            LOGGER.info("P2 生产就绪检查通过，checkCount={}", checks.size());
        } else {
            LOGGER.warn("P2 生产就绪检查未完全通过，checks={}", checks);
        }
        return report;
    }
}
