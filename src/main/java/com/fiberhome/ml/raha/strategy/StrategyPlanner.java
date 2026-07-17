package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据字段画像生成有限且确定的 OD 和 PVD 策略计划。
 */
public final class StrategyPlanner {

    /**
     * 为每个目标字段生成缺失、类型、长度和低频策略。
     *
     * @param profiles 字段画像
     * @return 冻结策略计划
     */
    public StrategyPlan plan(List<ColumnProfile> profiles) {
        List<StrategyDefinition> definitions = new ArrayList<StrategyDefinition>();
        for (ColumnProfile profile : profiles) {
            Map<String, Object> lengthParameters = new LinkedHashMap<String, Object>();
            lengthParameters.put("averageLength", profile.getAverageLength());
            add(definitions, "PVD", "MISSING", profile.getColumnName(), "{}");
            add(definitions, "PVD", "TYPE_FORMAT", profile.getColumnName(), "{}");
            add(definitions, "PVD", "LENGTH", profile.getColumnName(),
                    JsonUtils.toJson(lengthParameters));
            add(definitions, "OD", "LOW_FREQUENCY", profile.getColumnName(), "{}");
        }
        StringBuilder signature = new StringBuilder();
        for (StrategyDefinition definition : definitions) {
            signature.append(definition.getStrategyId()).append('|');
        }
        return new StrategyPlan("plan:" + HashUtils.sha256(signature.toString())
                .substring(0, 24), definitions);
    }

    private static void add(List<StrategyDefinition> definitions, String family,
                            String implementation, String column, String parameters) {
        String source = family + '|' + implementation + '|' + column + '|' + parameters;
        definitions.add(new StrategyDefinition(
                family.toLowerCase() + '_' + HashUtils.sha256(source).substring(0, 16),
                family, implementation, column, parameters));
    }
}
