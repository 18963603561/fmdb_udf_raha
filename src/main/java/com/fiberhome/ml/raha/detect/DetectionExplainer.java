package com.fiberhome.ml.raha.detect;

import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.support.JsonUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 生成检测结果的结构化原因摘要。
 */
public final class DetectionExplainer {

    public String explain(RahaColumnModel model) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("code", "MODEL_SCORE");
        values.put("classifier", model.getClassifierType().name());
        values.put("threshold", model.getThreshold());
        return JsonUtils.toJson(values);
    }
}
