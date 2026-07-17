package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.support.JsonUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 训练和检测共享的不可变策略计划。
 */
public final class StrategyPlan {

    /** 计划版本。 */
    private final String version;
    /** 按确定顺序排列的策略。 */
    private final List<StrategyDefinition> definitions;

    public StrategyPlan(String version, List<StrategyDefinition> definitions) {
        this.version = version;
        this.definitions = Collections.unmodifiableList(
                new ArrayList<StrategyDefinition>(definitions));
    }

    public String getVersion() { return version; }
    public List<StrategyDefinition> getDefinitions() { return definitions; }

    public String toJson() {
        List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
        for (StrategyDefinition definition : definitions) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("id", definition.getStrategyId());
            item.put("family", definition.getFamily());
            item.put("implementation", definition.getImplementation());
            item.put("target", definition.getTargetColumn());
            item.put("parameters", definition.getParametersJson());
            values.add(item);
        }
        return JsonUtils.toJson(values);
    }
}
