package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.util.FormDataCodec;

import java.util.Map;

/**
 * 保存检测 UDF 的公共输入和模型版本选择器。
 */
public final class RahaDetectUdfRequest {

    /** 三个入口共享的不可变输入字段。 */
    private final RahaUdfCommonFields commonFields;
    /** 精确模型版本或按字段选择当前发布模型的固定选择器。 */
    private final String modelVersion;

    public RahaDetectUdfRequest(RahaUdfCommonFields commonFields,
                                String modelVersion) {
        if (commonFields == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "检测 UDF 公共参数不能为空");
        }
        this.commonFields = commonFields;
        this.modelVersion = RahaUdfCommonFields.required(modelVersion,
                "modelVersion");
    }

    /**
     * 生成稳定检测配置文本，不包含调用方和幂等键。
     */
    public String toCanonicalConfiguration() {
        Map<String, String> values = commonFields.canonicalValues();
        values.put("operation", "DETECT");
        values.put("modelVersion", modelVersion);
        return FormDataCodec.encode(values);
    }

    public RahaUdfCommonFields getCommonFields() { return commonFields; }
    public String getModelVersion() { return modelVersion; }
    public String getDatasetId() { return commonFields.getDatasetId(); }
    public String getIdempotencyKey() { return commonFields.getIdempotencyKey(); }
    public String getCaller() { return commonFields.getCaller(); }
    public String getResultTable() { return commonFields.getResultTable(); }
}
