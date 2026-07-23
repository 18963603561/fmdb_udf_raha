package com.fiberhome.ml.raha.annotation.auto;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存大模型连接信息、上下文限制、重试和失败处理配置。
 */
public final class AutoAnnotationConfig {

    /** 默认密钥环境变量名。 */
    public static final String DEFAULT_API_KEY_ENV = "RAHA_AUTO_LABEL_API_KEY";
    /** 是否启用自动标注。 */
    private final boolean enabled;
    /** 大模型聊天补全接口完整地址。 */
    private final String modelUrl;
    /** 大模型接口密钥，仅在内存中使用。 */
    private final String apiKey;
    /** 接口请求使用的模型名称。 */
    private final String model;
    /** 模型采样温度。 */
    private final double temperature;
    /** 当前使用的模型容量档案名称。 */
    private final String modelCapacityProfile;
    /** 模型上下文窗口令牌数。 */
    private final int contextWindowTokens;
    /** 单批最大行数。 */
    private final int maxRowsPerBatch;
    /** 单批输入近似最大字符数。 */
    private final int maxCharsPerBatch;
    /** 单批最大可检测字段数。 */
    private final int maxColumnsPerBatch;
    /** 单个字段发送给模型的最大字符数。 */
    private final int maxValueChars;
    /** 最大并发模型调用数。 */
    private final int maxParallelBatches;
    /** 单批失败后的最大重试次数。 */
    private final int maxRetryCount;
    /** 单批连接和读取超时时间。 */
    private final int batchTimeoutMillis;
    /** 最大自动标注总行数，零表示不限制。 */
    private final int maxTotalRows;
    /** 单次模型响应最大字节数。 */
    private final int maxResponseBytes;
    /** 模型最大输出令牌数。 */
    private final int maxOutputTokens;
    /** 自动标注失败策略。 */
    private final AutoAnnotationFailPolicy failPolicy;
    /** 是否对敏感字段原值脱敏。 */
    private final boolean maskSensitiveColumns;

    private AutoAnnotationConfig(Map<String, String> values) {
        Map<String, String> source = values == null
                ? Collections.<String, String>emptyMap()
                : new LinkedHashMap<String, String>(values);
        this.enabled = bool(source, "autoLabelEnabled", false);
        this.modelUrl = trim(source.get("autoLabelModelUrl"));
        String apiKeyEnv = text(source, "autoLabelApiKeyEnv",
                DEFAULT_API_KEY_ENV);
        String explicitKey = trim(source.get("autoLabelApiKey"));
        this.apiKey = explicitKey == null ? trim(System.getenv(apiKeyEnv))
                : explicitKey;
        this.model = trim(source.get("autoLabelModel"));
        this.temperature = decimal(source, "autoLabelTemperature", 0.0D,
                0.0D, 2.0D);
        AutoAnnotationModelCapacity capacity =
                AutoAnnotationModelCapacity.resolve(this.model);
        boolean contextOverridden = trim(source.get(
                "autoLabelContextWindowTokens")) != null;
        this.modelCapacityProfile = contextOverridden
                ? capacity.getProfileName() + "_OVERRIDDEN"
                : capacity.getProfileName();
        this.contextWindowTokens = integer(source,
                "autoLabelContextWindowTokens",
                capacity.getContextWindowTokens(), 4096, 2000000);
        this.maxOutputTokens = integer(source, "autoLabelMaxOutputTokens",
                Math.min(capacity.getMaxOutputTokens(),
                        this.contextWindowTokens / 4), 128, 131072);
        validateTokenBudget();
        int inputTokenBudget = inputTokenBudget();
        this.maxRowsPerBatch = integer(source, "autoLabelMaxRowsPerBatch",
                defaultRows(this.maxOutputTokens), 1, 1000);
        this.maxCharsPerBatch = integer(source, "autoLabelMaxCharsPerBatch",
                defaultChars(inputTokenBudget), 1000, 1000000);
        this.maxColumnsPerBatch = integer(source,
                "autoLabelMaxColumnsPerBatch", defaultColumns(inputTokenBudget),
                1, 1000);
        this.maxValueChars = integer(source, "autoLabelMaxValueChars",
                1000, 32, 100000);
        this.maxParallelBatches = integer(source,
                "autoLabelMaxParallelBatches", 1, 1, 32);
        this.maxRetryCount = integer(source, "autoLabelMaxRetryCount",
                2, 0, 10);
        this.batchTimeoutMillis = integer(source,
                "autoLabelBatchTimeoutMillis", 120000, 1000, 1800000);
        this.maxTotalRows = integer(source, "autoLabelMaxTotalRows",
                0, 0, 50000);
        this.maxResponseBytes = integer(source, "autoLabelMaxResponseBytes",
                4 * 1024 * 1024, 1024, 32 * 1024 * 1024);
        this.failPolicy = AutoAnnotationFailPolicy.parse(
                source.get("autoLabelFailPolicy"));
        this.maskSensitiveColumns = bool(source,
                "autoLabelMaskSensitiveColumns", true);
    }

    /**
     * 从 UDF 字符串参数构造配置，关闭时不要求模型连接信息。
     *
     * @param values UDF 参数
     * @return 自动标注配置
     */
    public static AutoAnnotationConfig from(Map<String, String> values) {
        return new AutoAnnotationConfig(values);
    }

    /**
     * 在真正调用模型前校验连接配置。
     */
    public void validateEnabled() {
        if (!enabled) {
            return;
        }
        if (modelUrl == null) {
            throw new IllegalArgumentException("autoLabelModelUrl 不能为空");
        }
        try {
            URL url = new URL(modelUrl);
            if (!"http".equalsIgnoreCase(url.getProtocol())
                    && !"https".equalsIgnoreCase(url.getProtocol())) {
                throw new IllegalArgumentException(
                        "autoLabelModelUrl 只支持 HTTP 或 HTTPS");
            }
        } catch (MalformedURLException exception) {
            // 不保留底层异常，避免非法地址原文进入日志和审计报告。
            throw new IllegalArgumentException("autoLabelModelUrl 格式无效");
        }
        if (apiKey == null) {
            throw new IllegalArgumentException(
                    "自动标注密钥不能为空，请传 autoLabelApiKey 或配置密钥环境变量");
        }
        if (model == null) {
            throw new IllegalArgumentException("autoLabelModel 不能为空");
        }
    }

    public boolean isEnabled() { return enabled; }
    public String getModelUrl() { return modelUrl; }
    public String getApiKey() { return apiKey; }
    public String getModel() { return model; }
    public double getTemperature() { return temperature; }
    public String getModelCapacityProfile() { return modelCapacityProfile; }
    public int getContextWindowTokens() { return contextWindowTokens; }
    public int getMaxRowsPerBatch() { return maxRowsPerBatch; }
    public int getMaxCharsPerBatch() { return maxCharsPerBatch; }
    public int getMaxColumnsPerBatch() { return maxColumnsPerBatch; }
    public int getMaxValueChars() { return maxValueChars; }
    public int getMaxParallelBatches() { return maxParallelBatches; }
    public int getMaxRetryCount() { return maxRetryCount; }
    public int getBatchTimeoutMillis() { return batchTimeoutMillis; }
    public int getMaxTotalRows() { return maxTotalRows; }
    public int getMaxResponseBytes() { return maxResponseBytes; }
    public int getMaxOutputTokens() { return maxOutputTokens; }
    public AutoAnnotationFailPolicy getFailPolicy() { return failPolicy; }
    public boolean isMaskSensitiveColumns() { return maskSensitiveColumns; }

    private void validateTokenBudget() {
        int reserve = safetyReserveTokens(contextWindowTokens);
        if (maxOutputTokens + reserve >= contextWindowTokens) {
            throw new IllegalArgumentException(
                    "autoLabelMaxOutputTokens 与安全预留之和必须小于上下文窗口");
        }
    }

    private int inputTokenBudget() {
        return contextWindowTokens - maxOutputTokens
                - safetyReserveTokens(contextWindowTokens);
    }

    private static int safetyReserveTokens(int contextTokens) {
        return Math.max(2048, contextTokens / 20);
    }

    private static int defaultChars(int inputTokenBudget) {
        return Math.max(1000, Math.min(1000000,
                (int) Math.floor(inputTokenBudget * 0.75D)));
    }

    private static int defaultRows(int maxOutputTokens) {
        return Math.max(1, Math.min(500,
                (int) Math.floor(maxOutputTokens * 0.70D / 128.0D)));
    }

    private static int defaultColumns(int inputTokenBudget) {
        return Math.max(10, Math.min(200, inputTokenBudget / 512));
    }

    private static int integer(Map<String, String> values, String key,
                               int defaultValue, int minimum, int maximum) {
        String value = trim(values.get(key));
        int parsed = defaultValue;
        if (value != null) {
            try {
                parsed = Integer.parseInt(value);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(key + " 必须为整数", exception);
            }
        }
        if (parsed < minimum || parsed > maximum) {
            throw new IllegalArgumentException(key + " 必须位于 " + minimum
                    + " 到 " + maximum + " 之间");
        }
        return parsed;
    }

    private static double decimal(Map<String, String> values, String key,
                                  double defaultValue, double minimum,
                                  double maximum) {
        String value = trim(values.get(key));
        double parsed = defaultValue;
        if (value != null) {
            try {
                parsed = Double.parseDouble(value);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException(key + " 必须为数字", exception);
            }
        }
        if (Double.isNaN(parsed) || parsed < minimum || parsed > maximum) {
            throw new IllegalArgumentException(key + " 必须位于 " + minimum
                    + " 到 " + maximum + " 之间");
        }
        return parsed;
    }

    private static boolean bool(Map<String, String> values, String key,
                                boolean defaultValue) {
        String value = trim(values.get(key));
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value) || "1".equals(value)
                || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value)
                || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException(key + " 必须为 true 或 false");
    }

    private static String text(Map<String, String> values, String key,
                               String defaultValue) {
        String value = trim(values.get(key));
        return value == null ? defaultValue : value;
    }

    private static String trim(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
