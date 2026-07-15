package com.fiberhome.ml.raha.production;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存 P2 生产验收检查项、是否完全就绪以及待完成项。
 */
public final class ProductionReadinessReport {

    /** 按检查项名称保存的通过状态。 */
    private final Map<String, Boolean> checks;
    /** 所有检查项是否通过。 */
    private final boolean ready;

    public ProductionReadinessReport(Map<String, Boolean> checks) {
        if (checks == null || checks.isEmpty()) {
            throw new IllegalArgumentException("生产验收检查项不能为空");
        }
        boolean allReady = true;
        for (Boolean value : checks.values()) {
            if (value == null) {
                throw new IllegalArgumentException("生产验收检查状态不能为空");
            }
            allReady = allReady && value;
        }
        this.checks = Collections.unmodifiableMap(
                new LinkedHashMap<String, Boolean>(checks));
        this.ready = allReady;
    }

    public Map<String, Boolean> getChecks() { return checks; }
    public boolean isReady() { return ready; }
}
