package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.util.FormDataCodec;

import java.util.Map;

/**
 * 保存采样 UDF 的公共输入和标注预算。
 */
public final class RahaSampleUdfRequest {

    /** 三个入口共享的不可变输入字段。 */
    private final RahaUdfCommonFields commonFields;
    /** 本次最多选择的待标注行数。 */
    private final int labelingBudget;

    public RahaSampleUdfRequest(RahaUdfCommonFields commonFields,
                                int labelingBudget) {
        if (commonFields == null) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "采样 UDF 公共参数不能为空");
        }
        if (labelingBudget <= 0) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "采样 UDF 的 labelingBudget 必须大于零");
        }
        this.commonFields = commonFields;
        this.labelingBudget = labelingBudget;
    }

    /**
     * 生成稳定采样配置文本，不包含调用方和幂等键。
     */
    public String toCanonicalConfiguration() {
        Map<String, String> values = commonFields.canonicalValues();
        values.put("operation", "SAMPLE");
        values.put("labelingBudget", String.valueOf(labelingBudget));
        return FormDataCodec.encode(values);
    }

    public RahaUdfCommonFields getCommonFields() { return commonFields; }
    public int getLabelingBudget() { return labelingBudget; }
    public String getDatasetId() { return commonFields.getDatasetId(); }
    public String getIdempotencyKey() { return commonFields.getIdempotencyKey(); }
    public String getCaller() { return commonFields.getCaller(); }
    public String getResultTable() { return commonFields.getResultTable(); }
}
