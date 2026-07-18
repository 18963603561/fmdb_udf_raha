package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.HashUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将 Java 策略配置和逐策略命中坐标写为可与 Python 比较的 JSONL 产物。
 */
public final class StrategyAlignmentArtifactWriter {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            StrategyAlignmentArtifactWriter.class);

    /**
     * 按策略计划顺序写出规范配置、命中数量、坐标摘要和完整坐标。
     *
     * @param outputPath 输出 JSONL 路径
     * @param plans 当前策略计划
     * @param batch 当前策略执行结果
     */
    public void write(Path outputPath,
                      List<StrategyPlan> plans,
                      StrategyBatchResult batch) {
        if (outputPath == null || plans == null || batch == null) {
            throw new IllegalArgumentException("策略对齐输出路径、计划和结果不能为空");
        }
        Map<String, StrategyExecutionResult> executions =
                new LinkedHashMap<String, StrategyExecutionResult>();
        for (StrategyExecutionResult execution : batch.getExecutions()) {
            executions.put(execution.getSummary().getStrategyId(), execution);
        }
        StringBuilder content = new StringBuilder();
        for (StrategyPlan plan : plans) {
            StrategyExecutionResult execution = executions.get(plan.getStrategyId());
            if (execution == null) {
                throw new IllegalStateException("策略对齐产物缺少执行结果："
                        + plan.getStrategyId());
            }
            List<String> coordinates = new ArrayList<String>();
            for (StrategyHit hit : execution.getHits()) {
                coordinates.add(hit.getCoordinate().getRowId() + "|"
                        + hit.getCoordinate().getColumnName());
            }
            Collections.sort(coordinates);
            content.append('{')
                    .append("\"source\":\"JAVA\",")
                    .append("\"family\":\"").append(plan.getStrategyFamily().name())
                    .append("\",")
                    .append("\"strategyId\":\"").append(plan.getStrategyId())
                    .append("\",")
                    .append("\"strategyType\":\"").append(escape(plan.getConfiguration()
                            .get(StrategyConfigurationKeys.STRATEGY_TYPE))).append("\",")
                    .append("\"canonicalConfiguration\":\"")
                    .append(escape(canonicalConfiguration(plan))).append("\",")
                    .append("\"configurationHash\":\"")
                    .append(plan.getConfigurationHash()).append("\",")
                    .append("\"status\":\"")
                    .append(execution.getSummary().getStatus().name()).append("\",")
                    .append("\"candidateCount\":").append(coordinates.size()).append(',')
                    .append("\"coordinateHash\":\"")
                    .append(HashUtils.sha256Hex(String.join("\n", coordinates)))
                    .append("\",")
                    .append("\"runtimeMillis\":")
                    .append(execution.getSummary().getRuntimeMillis()).append(',')
                    .append("\"coordinates\":[");
            for (int index = 0; index < coordinates.size(); index++) {
                if (index > 0) {
                    content.append(',');
                }
                content.append('"').append(escape(coordinates.get(index))).append('"');
            }
            content.append("]}\n");
        }
        try {
            Files.createDirectories(outputPath.toAbsolutePath().normalize().getParent());
            Files.write(outputPath, content.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            LOGGER.info("Java 策略对齐产物写出完成，outputPath={}，strategyCount={}",
                    outputPath, plans.size());
        } catch (IOException exception) {
            LOGGER.error("Java 策略对齐产物写出失败，outputPath={}",
                    outputPath, exception);
            throw new IllegalStateException("无法写出 Java 策略对齐产物", exception);
        }
    }

    private static String canonicalConfiguration(StrategyPlan plan) {
        StringBuilder value = new StringBuilder(plan.getStrategyFamily().name());
        for (String column : plan.getTargetColumns()) {
            value.append('|').append(column);
        }
        for (Map.Entry<String, String> entry
                : new TreeMap<String, String>(plan.getConfiguration()).entrySet()) {
            value.append('|').append(entry.getKey()).append('=').append(entry.getValue());
        }
        return value.toString();
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
