package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.util.FormDataCodec;

import java.util.Map;

/**
 * 保存训练 UDF 的公共输入和标注表引用。
 */
public final class RahaTrainUdfRequest {

    /** 三个入口共享的不可变输入字段。 */
    private final RahaUdfCommonFields commonFields;
    /** 训练使用的 FMDB 标注表。 */
    private final String annotationReference;

    public RahaTrainUdfRequest(RahaUdfCommonFields commonFields,
                               String annotationReference) {
        if (commonFields == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "训练 UDF 公共参数不能为空");
        }
        this.commonFields = commonFields;
        this.annotationReference = RahaUdfCommonFields.validateTable(
                RahaUdfCommonFields.required(annotationReference,
                        "annotationReference"), "annotationReference");
    }

    /**
     * 生成稳定训练配置文本，不包含调用方和幂等键。
     */
    public String toCanonicalConfiguration() {
        Map<String, String> values = commonFields.canonicalValues();
        values.put("operation", "TRAIN");
        values.put("annotationReference", annotationReference);
        return FormDataCodec.encode(values);
    }

    public RahaUdfCommonFields getCommonFields() { return commonFields; }
    public String getAnnotationReference() { return annotationReference; }
    public String getDatasetId() { return commonFields.getDatasetId(); }
    public String getIdempotencyKey() { return commonFields.getIdempotencyKey(); }
    public String getCaller() { return commonFields.getCaller(); }
    public String getResultTable() { return commonFields.getResultTable(); }
}
