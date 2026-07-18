package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.util.FormDataCodec;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 解析严格表单编码的 UDF 请求并拒绝未知或跨任务参数。
 */
public final class RahaUdfRequestParser implements Serializable {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 单次 UDF 请求允许的固定最大字符数。 */
    private static final int DEFAULT_MAX_REQUEST_LENGTH = 65536;
    /** 单次 UDF 请求允许的最大字符数。 */
    private final int maxRequestLength;

    /** 三类请求都允许出现的字段。 */
    private static final Set<String> COMMON_KEYS = keys(
            "datasetId", "inputReference", "sourceType", "rowIdColumn",
            "snapshotId", "idempotencyKey", "caller", "resultTable");
    /** 采样请求字段白名单。 */
    private static final Set<String> SAMPLE_KEYS = keys(COMMON_KEYS,
            "labelingBudget");
    /** 训练请求字段白名单。 */
    private static final Set<String> TRAIN_KEYS = keys(COMMON_KEYS,
            "annotationReference");
    /** 检测请求字段白名单。 */
    private static final Set<String> DETECT_KEYS = keys(COMMON_KEYS,
            "modelVersion");
    /** 所有已定义字段，用于区分未知字段和跨任务字段。 */
    private static final Set<String> KNOWN_KEYS = keys(COMMON_KEYS,
            "labelingBudget", "annotationReference", "modelVersion");

    public RahaUdfRequestParser() {
        this(DEFAULT_MAX_REQUEST_LENGTH);
    }

    public RahaUdfRequestParser(int maxRequestLength) {
        if (maxRequestLength <= 0) {
            throw new IllegalArgumentException("UDF 请求长度上限必须大于 0");
        }
        this.maxRequestLength = maxRequestLength;
    }

    /**
     * 解析采样入口请求。
     *
     * @param encodedRequest 表单编码请求
     * @return 强类型采样请求
     */
    public RahaSampleUdfRequest parseSample(String encodedRequest) {
        Map<String, String> values = decode(encodedRequest);
        validateKeys(values, SAMPLE_KEYS, "采样");
        return new RahaSampleUdfRequest(commonFields(values),
                integerValue(values.get("labelingBudget")));
    }

    /**
     * 解析训练入口请求。
     *
     * @param encodedRequest 表单编码请求
     * @return 强类型训练请求
     */
    public RahaTrainUdfRequest parseTrain(String encodedRequest) {
        Map<String, String> values = decode(encodedRequest);
        validateKeys(values, TRAIN_KEYS, "训练");
        return new RahaTrainUdfRequest(commonFields(values),
                values.get("annotationReference"));
    }

    /**
     * 解析检测入口请求。
     *
     * @param encodedRequest 表单编码请求
     * @return 强类型检测请求
     */
    public RahaDetectUdfRequest parseDetect(String encodedRequest) {
        Map<String, String> values = decode(encodedRequest);
        validateKeys(values, DETECT_KEYS, "检测");
        return new RahaDetectUdfRequest(commonFields(values),
                values.get("modelVersion"));
    }

    private Map<String, String> decode(String encodedRequest) {
        if (encodedRequest != null && encodedRequest.length() > maxRequestLength) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 请求长度超过上限");
        }
        Map<String, String> values;
        try {
            values = FormDataCodec.decode(encodedRequest);
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 请求不是合法表单编码", exception);
        }
        return values;
    }

    private static void validateKeys(Map<String, String> values,
                                     Set<String> allowedKeys,
                                     String operationName) {
        for (String key : values.keySet()) {
            if (!KNOWN_KEYS.contains(key)) {
                throw new RahaUdfException("UNKNOWN_UDF_ARGUMENT",
                        "UDF 请求包含未知参数：" + key);
            }
            // 已定义但不属于当前入口的字段属于跨任务输入，必须在解析边界拒绝。
            if (!allowedKeys.contains(key)) {
                throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                        operationName + " UDF 不能包含参数：" + key);
            }
        }
    }

    private static RahaUdfCommonFields commonFields(Map<String, String> values) {
        return new RahaUdfCommonFields(values.get("datasetId"),
                values.get("inputReference"), sourceType(values.get("sourceType")),
                values.get("rowIdColumn"),
                values.get("snapshotId"), values.get("idempotencyKey"),
                values.get("caller"), values.get("resultTable"));
    }

    private static DataFormat sourceType(String value) {
        if ("TABLE".equalsIgnoreCase(value)) {
            return DataFormat.FMDB_TABLE;
        }
        if ("SQL".equalsIgnoreCase(value)) {
            return DataFormat.FMDB_SQL;
        }
        throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                "sourceType 只允许 TABLE 或 SQL");
    }

    private static int integerValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "labelingBudget 必须为整数", exception);
        }
    }

    private static Set<String> keys(String... values) {
        return Collections.unmodifiableSet(new LinkedHashSet<String>(
                Arrays.asList(values)));
    }

    private static Set<String> keys(Set<String> common, String... values) {
        Set<String> keys = new LinkedHashSet<String>(common);
        keys.addAll(Arrays.asList(values));
        return Collections.unmodifiableSet(keys);
    }
}
