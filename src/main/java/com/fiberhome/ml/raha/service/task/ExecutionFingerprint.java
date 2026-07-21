package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 保存任务请求的基础执行输入指纹和最终执行输入指纹。
 *
 * <p>基础指纹只由稳定业务输入生成，用于血缘归组；最终指纹会在强制运行时追加请求级运行标识，
 * 用于进入任务配置版本和幂等键计算。</p>
 */
public final class ExecutionFingerprint {

    /** 多批次追加后选择当前结果的统一规则。 */
    public static final String CURRENT_SELECT_RULE =
            "LATEST_SUCCESS_BY_BASE_FINGERPRINT";

    /** 不含强制运行标识的稳定业务输入指纹。 */
    private final String baseExecutionInputFingerprint;
    /** 实际写入 RahaJobConfig 并参与幂等计算的最终执行输入指纹。 */
    private final String executionInputFingerprint;
    /** 本次请求是否强制跳出默认幂等域。 */
    private final boolean forceRun;
    /** 调用方传入的强制运行幂等标识。 */
    private final String forceRunId;
    /** 参与最终指纹计算的请求级运行标识。 */
    private final String runNonce;

    private ExecutionFingerprint(String baseExecutionInputFingerprint,
                                 String executionInputFingerprint,
                                 boolean forceRun,
                                 String forceRunId,
                                 String runNonce) {
        this.baseExecutionInputFingerprint = ValueUtils.requireNotBlank(
                baseExecutionInputFingerprint, "基础执行输入指纹");
        this.executionInputFingerprint = ValueUtils.requireNotBlank(
                executionInputFingerprint, "最终执行输入指纹");
        this.forceRun = forceRun;
        this.forceRunId = trimToNull(forceRunId);
        this.runNonce = trimToNull(runNonce);
        if (forceRun && this.runNonce == null) {
            throw new IllegalArgumentException("强制运行必须包含运行标识");
        }
    }

    /**
     * 根据稳定业务输入源和覆盖选项生成执行指纹。
     *
     * @param stableSource 不包含强制运行参数的稳定输入源
     * @param options 执行覆盖选项
     * @return 可进入任务请求的指纹结果
     */
    public static ExecutionFingerprint fromStableSource(
            String stableSource,
            ExecutionOverrideOptions options) {
        String base = HashUtils.md5Hex(
                ValueUtils.requireNotBlank(stableSource, "稳定输入源"));
        ExecutionOverrideOptions effective = options == null
                ? ExecutionOverrideOptions.DEFAULT : options;
        if (!effective.isForceRun()) {
            return stable(base);
        }
        String nonce = effective.getForceRunId() == null
                ? generatedRunNonce() : effective.getForceRunId();
        StringBuilder source = new StringBuilder();
        appendToken(source, base);
        appendToken(source, "forceRun");
        appendToken(source, nonce);
        return new ExecutionFingerprint(base, HashUtils.md5Hex(
                source.toString()), true, effective.getForceRunId(), nonce);
    }

    /**
     * 使用已存在的最终指纹构造默认元数据。
     *
     * @param executionInputFingerprint 已写入配置的执行输入指纹
     * @return 不强制运行的指纹元数据
     */
    public static ExecutionFingerprint fromConfig(String executionInputFingerprint) {
        String value = ValueUtils.requireNotBlank(
                executionInputFingerprint, "配置执行输入指纹");
        return stable(value);
    }

    private static ExecutionFingerprint stable(String fingerprint) {
        String value = ValueUtils.requireNotBlank(fingerprint, "执行输入指纹");
        return new ExecutionFingerprint(value, value, false, null, null);
    }

    public String getBaseExecutionInputFingerprint() {
        return baseExecutionInputFingerprint;
    }

    public String getExecutionInputFingerprint() {
        return executionInputFingerprint;
    }

    public boolean isForceRun() {
        return forceRun;
    }

    public String getForceRunId() {
        return forceRunId;
    }

    public String getRunNonce() {
        return runNonce;
    }

    /**
     * 转换为可写入任务摘要或 UDF 返回行的稳定字段集合。
     *
     * @return 指纹和强制运行元数据
     */
    public Map<String, Object> toSummaryMap() {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("baseExecutionInputFingerprint",
                baseExecutionInputFingerprint);
        result.put("executionInputFingerprint", executionInputFingerprint);
        result.put("forceRun", Boolean.valueOf(forceRun));
        result.put("forceRunId", forceRunId);
        result.put("runNonce", runNonce);
        result.put("currentSelectRule", CURRENT_SELECT_RULE);
        return result;
    }

    private static String generatedRunNonce() {
        return String.valueOf(System.currentTimeMillis()) + "-"
                + UUID.randomUUID().toString();
    }

    private static void appendToken(StringBuilder source, Object value) {
        String text = value == null ? "<null>" : String.valueOf(value);
        source.append(text.length()).append(':').append(text);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
