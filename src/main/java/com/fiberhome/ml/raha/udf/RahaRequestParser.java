package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.api.DetectRequest;
import com.fiberhome.ml.raha.api.SampleRequest;
import com.fiberhome.ml.raha.api.TrainRequest;
import com.fiberhome.ml.raha.support.FormCodec;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 三个表级入口的固定字段请求解析器。
 */
public final class RahaRequestParser {

    /** 入口允许的最大请求长度。 */
    private static final int MAXIMUM_REQUEST_LENGTH = 8192;

    public SampleRequest parseSample(String encoded) {
        Map<String, String> values = decode(encoded);
        allow(values, "inputReference", "datasetId", "sourceType", "rowKeyColumns",
                "snapshotId", "targetColumns", "labelingBudget");
        return new SampleRequest(required(values, "inputReference"), values.get("datasetId"),
                values.get("sourceType"), list(values.get("rowKeyColumns")),
                values.get("snapshotId"), list(values.get("targetColumns")),
                integer(values.get("labelingBudget"), 0));
    }

    public TrainRequest parseTrain(String encoded) {
        Map<String, String> values = decode(encoded);
        allow(values, "sampleBatchIds", "targetColumns", "baseModelSetVersion");
        return new TrainRequest(list(required(values, "sampleBatchIds")),
                list(values.get("targetColumns")), values.get("baseModelSetVersion"));
    }

    public DetectRequest parseDetect(String encoded) {
        Map<String, String> values = decode(encoded);
        allow(values, "inputReference", "modelSetVersion", "sourceType",
                "rowKeyColumns", "snapshotId", "targetColumns", "errorsOnly");
        return new DetectRequest(required(values, "inputReference"),
                required(values, "modelSetVersion"), values.get("sourceType"),
                list(values.get("rowKeyColumns")), values.get("snapshotId"),
                list(values.get("targetColumns")), bool(values.get("errorsOnly"), true));
    }

    private static Map<String, String> decode(String encoded) {
        if (encoded == null || encoded.length() > MAXIMUM_REQUEST_LENGTH) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                    "请求为空或超过长度限制");
        }
        return FormCodec.decode(encoded);
    }

    private static void allow(Map<String, String> values, String... allowed) {
        Set<String> names = new HashSet<String>(Arrays.asList(allowed));
        for (String key : values.keySet()) {
            if (!names.contains(key)) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                        "请求包含未知参数：" + key);
            }
        }
    }

    private static String required(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                    "缺少必填参数：" + key);
        }
        return value.trim();
    }

    private static List<String> list(String value) {
        if (value == null || value.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        for (String token : value.split(",")) {
            if (!token.trim().isEmpty()) {
                result.add(token.trim());
            }
        }
        return result;
    }

    private static int integer(String value, int defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                    "整数参数格式不正确：" + value, exception);
        }
    }

    private static boolean bool(String value, boolean defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                    "布尔参数格式不正确：" + value);
        }
        return Boolean.parseBoolean(value);
    }
}
