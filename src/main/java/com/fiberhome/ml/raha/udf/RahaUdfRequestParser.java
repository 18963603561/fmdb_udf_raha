package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.util.FormDataCodec;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.io.Serializable;

/**
 * 解析严格表单编码的 UDF 请求并拒绝未知或跨任务参数。
 */
public final class RahaUdfRequestParser implements Serializable {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 单次 UDF 请求允许的最大字符数。 */
    private final int maxRequestLength;

    /**
     * UDF 请求允许出现的表单字段白名单，用于拒绝拼写错误、无效或跨任务传入的参数。
     * 例如：datasetId=dataset-001&inputReference=/data/input.csv&sourceType=CSV&labelingBudget=100。
     */
    private static final Set<String> ALLOWED_KEYS = new LinkedHashSet<String>(Arrays.asList(
            "datasetId", "inputReference", "sourceType", "rowIdColumn", "snapshotId",
            "idempotencyKey", "caller", "resultTable", "annotationReference",
            "modelVersion", "labelingBudget"));

    public RahaUdfRequestParser() {
        this(RahaDefaultConfigProvider.factory().udfConfig().getMaxRequestLength());
    }

    public RahaUdfRequestParser(int maxRequestLength) {
        if (maxRequestLength <= 0) {
            throw new IllegalArgumentException("UDF 请求长度上限必须大于 0");
        }
        this.maxRequestLength = maxRequestLength;
    }

    public RahaUdfRequest parse(RahaTaskType taskType, String encodedRequest) {
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
        for (String key : values.keySet()) {
            if (!ALLOWED_KEYS.contains(key)) {
                throw new RahaUdfException("UNKNOWN_UDF_ARGUMENT",
                        "UDF 请求包含未知参数：" + key);
            }
        }
        DataFormat sourceType = sourceType(values.get("sourceType"));
        int budget = integerValue(values.get("labelingBudget"));
        return new RahaUdfRequest(taskType, values.get("datasetId"),
                values.get("inputReference"), sourceType, values.get("rowIdColumn"),
                values.get("snapshotId"), values.get("idempotencyKey"),
                values.get("caller"), values.get("resultTable"),
                values.get("annotationReference"), values.get("modelVersion"), budget);
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
}
