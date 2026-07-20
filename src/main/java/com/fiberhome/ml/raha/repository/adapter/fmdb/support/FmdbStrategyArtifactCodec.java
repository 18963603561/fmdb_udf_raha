package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 定义字段策略计划和执行摘要在列级产物表中的合并 JSON 协议。
 */
public final class FmdbStrategyArtifactCodec {

    private FmdbStrategyArtifactCodec() {
    }

    public static String write(List<StrategyPlan> plans,
                               List<StrategyRunSummary> summaries) {
        if (plans == null || summaries == null) {
            throw new IllegalArgumentException("策略计划和摘要不能为空");
        }
        Map<String, Object> root = new LinkedHashMap<String, Object>();
        root.put("plans", plans);
        root.put("summaries", summaries);
        return FmdbJsonCodec.write(root);
    }

    public static List<StrategyPlan> readPlans(String json) {
        List<Object> raw = array(json, "plans");
        List<StrategyPlan> plans = new ArrayList<StrategyPlan>(raw.size());
        for (Object item : raw) {
            Map<String, Object> values = FmdbJsonValue.objectMap(item, "plans");
            plans.add(new StrategyPlan(
                    FmdbJsonValue.requiredText(values, "strategyId"),
                    StrategyFamily.valueOf(FmdbJsonValue.requiredText(
                            values, "strategyFamily")),
                    FmdbJsonValue.stringList(values, "targetColumns"),
                    FmdbJsonValue.stringMap(values, "configuration"),
                    FmdbJsonValue.requiredNumber(values, "priority").intValue(),
                    StrategyStatus.valueOf(FmdbJsonValue.requiredText(values, "status"))));
        }
        return Collections.unmodifiableList(plans);
    }

    public static List<StrategyRunSummary> readSummaries(String json) {
        List<Object> raw = array(json, "summaries");
        List<StrategyRunSummary> summaries =
                new ArrayList<StrategyRunSummary>(raw.size());
        for (Object item : raw) {
            Map<String, Object> values = FmdbJsonValue.objectMap(item, "summaries");
            summaries.add(new StrategyRunSummary(
                    FmdbJsonValue.requiredText(values, "jobId"),
                    FmdbJsonValue.requiredText(values, "stageId"),
                    FmdbJsonValue.requiredText(values, "snapshotId"),
                    FmdbJsonValue.requiredText(values, "strategyId"),
                    FmdbJsonValue.requiredText(values, "configurationHash"),
                    StrategyFamily.valueOf(FmdbJsonValue.requiredText(
                            values, "strategyFamily")),
                    StrategyStatus.valueOf(FmdbJsonValue.requiredText(values, "status")),
                    FmdbJsonValue.requiredNumber(values, "inputCount").longValue(),
                    FmdbJsonValue.requiredNumber(values, "hitCount").longValue(),
                    FmdbJsonValue.requiredNumber(values, "runtimeMillis").longValue(),
                    FmdbJsonValue.optionalText(values, "errorCode"),
                    FmdbJsonValue.optionalText(values, "errorMessage"),
                    FmdbJsonValue.requiredNumber(values, "completedAt").longValue()));
        }
        return Collections.unmodifiableList(summaries);
    }

    private static List<Object> array(String json, String key) {
        if (json == null || json.trim().isEmpty()) {
            return Collections.emptyList();
        }
        // 兼容首个实现中直接保存策略计划数组的历史记录。
        if (json.trim().startsWith("[")) {
            return "plans".equals(key) ? FmdbJsonCodec.readArray(json)
                    : Collections.emptyList();
        }
        return FmdbJsonValue.objectList(FmdbJsonCodec.readObject(json), key);
    }
}
