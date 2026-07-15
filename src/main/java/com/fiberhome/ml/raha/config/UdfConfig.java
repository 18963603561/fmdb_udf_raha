package com.fiberhome.ml.raha.config;

import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.regex.Pattern;

/**
 * 控制 Raha 表级 UDF 的注册名称和单次请求长度上限。
 */
public final class UdfConfig {

    /** Spark SQL 函数名称格式。 */
    private static final Pattern FUNCTION_NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*");
    /** 训练 UDF 名称。 */
    private final String trainFunction;
    /** 检测 UDF 名称。 */
    private final String detectFunction;
    /** 采样 UDF 名称。 */
    private final String sampleFunction;
    /** 单次请求最大字符数。 */
    private final int maxRequestLength;

    public UdfConfig(String trainFunction,
                     String detectFunction,
                     String sampleFunction,
                     int maxRequestLength) {
        this.trainFunction = functionName(trainFunction, "训练 UDF 名称");
        this.detectFunction = functionName(detectFunction, "检测 UDF 名称");
        this.sampleFunction = functionName(sampleFunction, "采样 UDF 名称");
        if (this.trainFunction.equals(this.detectFunction)
                || this.trainFunction.equals(this.sampleFunction)
                || this.detectFunction.equals(this.sampleFunction)
                || maxRequestLength <= 0) {
            throw new IllegalArgumentException("UDF 名称必须唯一且请求长度上限必须大于 0");
        }
        this.maxRequestLength = maxRequestLength;
    }

    private static String functionName(String value, String fieldName) {
        String validated = ValueUtils.requireNotBlank(value, fieldName);
        if (!FUNCTION_NAME.matcher(validated).matches()) {
            throw new IllegalArgumentException(fieldName + "格式非法");
        }
        return validated;
    }

    public String getTrainFunction() { return trainFunction; }
    public String getDetectFunction() { return detectFunction; }
    public String getSampleFunction() { return sampleFunction; }
    public int getMaxRequestLength() { return maxRequestLength; }
}
