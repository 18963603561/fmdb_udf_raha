package com.fiberhome.ml.raha.strategy.plan;

import com.fiberhome.ml.raha.util.HashUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据策略标识和配置摘要生成与顺序无关的稳定计划版本。
 */
public final class StrategyPlanVersioner {

    private StrategyPlanVersioner() {
    }

    /**
     * 计算策略计划稳定版本。
     *
     * @param plans 策略计划列表，允许为空列表
     * @return 策略计划稳定摘要
     */
    public static String versionOf(List<StrategyPlan> plans) {
        if (plans == null) {
            throw new IllegalArgumentException("策略计划列表不能为空");
        }
        List<String> signatures = new ArrayList<String>(plans.size());
        for (StrategyPlan plan : plans) {
            if (plan == null) {
                throw new IllegalArgumentException("策略计划不能包含空值");
            }
            signatures.add(plan.getStrategyId() + ":" + plan.getConfigurationHash());
        }
        Collections.sort(signatures);
        return HashUtils.md5Hex(String.join("|", signatures));
    }
}
