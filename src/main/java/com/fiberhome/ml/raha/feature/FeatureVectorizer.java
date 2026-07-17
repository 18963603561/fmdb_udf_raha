package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.ValueNormalizer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 使用冻结字典把原始单元格值转换为稳定稀疏特征。
 */
public final class FeatureVectorizer {

    /**
     * 生成固定维度特征向量。
     *
     * @param value 原始值
     * @param dictionary 冻结字典
     * @return 稠密特征数组
     */
    public double[] vectorize(String value, FeatureDictionary dictionary) {
        String normalized = ValueNormalizer.normalize(value);
        double[] vector = new double[dictionary.size()];
        vector[0] = normalized.isEmpty() ? 1.0d : 0.0d;
        vector[1] = ValueNormalizer.isNumeric(normalized) ? 1.0d : 0.0d;
        vector[2] = Math.min(1.0d, normalized.length() / 100.0d);
        vector[3] = normalized.matches(".*\\d.*") ? 1.0d : 0.0d;
        vector[4] = normalized.matches(".*\\s.*") ? 1.0d : 0.0d;
        Integer valueIndex = dictionary.indexOf("value=" + normalized);
        if (valueIndex != null) {
            vector[valueIndex] = 1.0d;
        }
        return vector;
    }

    public String toSparseJson(double[] vector) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        for (int index = 0; index < vector.length; index++) {
            if (vector[index] != 0.0d) {
                values.put(String.valueOf(index), vector[index]);
            }
        }
        return JsonUtils.toJson(values);
    }

    public double[] fromSparseJson(String json, int dimension) {
        double[] vector = new double[dimension];
        for (Map.Entry<String, String> entry : JsonUtils.parseStringMap(json).entrySet()) {
            int index = Integer.parseInt(entry.getKey());
            if (index >= 0 && index < dimension) {
                vector[index] = Double.parseDouble(entry.getValue());
            }
        }
        return vector;
    }
}
