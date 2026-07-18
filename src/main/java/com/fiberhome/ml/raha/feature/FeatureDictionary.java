package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.ValueNormalizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 单列稳定特征字典，包含固定结构特征和训练值特征。
 */
public final class FeatureDictionary {

    /** 固定特征数量。 */
    public static final int FIXED_FEATURE_COUNT = 5;
    /** 目标字段。 */
    private final String columnName;
    /** 特征字典版本。 */
    private final String version;
    /** 按编号排序的特征名称。 */
    private final List<String> featureNames;
    /** 特征名称到编号的索引。 */
    private final Map<String, Integer> indexByName;

    public FeatureDictionary(String columnName, List<String> featureNames) {
        this.columnName = columnName;
        this.featureNames = Collections.unmodifiableList(new ArrayList<String>(featureNames));
        this.indexByName = new LinkedHashMap<String, Integer>();
        for (int index = 0; index < featureNames.size(); index++) {
            indexByName.put(featureNames.get(index), index);
        }
        this.version = "dict:" + HashUtils.sha256(columnName + '|'
                + JsonUtils.toJson(featureNames)).substring(0, 24);
    }

    /**
     * 从训练值构建确定字典，离散值按字典序编号。
     *
     * @param columnName 字段名
     * @param values 训练值
     * @param maximumValues 最大离散值数量
     * @return 特征字典
     */
    public static FeatureDictionary build(String columnName, List<String> values,
                                          int maximumValues) {
        List<String> names = new ArrayList<String>();
        names.add("shape.missing");
        names.add("shape.numeric");
        names.add("shape.length");
        names.add("shape.has_digit");
        names.add("shape.has_space");
        Set<String> distinct = new LinkedHashSet<String>();
        for (String value : values) {
            distinct.add(ValueNormalizer.normalize(value));
        }
        List<String> sorted = new ArrayList<String>(distinct);
        Collections.sort(sorted, Comparator.naturalOrder());
        int limit = Math.min(maximumValues, sorted.size());
        for (int index = 0; index < limit; index++) {
            names.add("value=" + sorted.get(index));
        }
        return new FeatureDictionary(columnName, names);
    }

    public String getColumnName() { return columnName; }
    public String getVersion() { return version; }
    public List<String> getFeatureNames() { return featureNames; }
    public int size() { return featureNames.size(); }

    public Integer indexOf(String name) {
        return indexByName.get(name);
    }

    public String toJson() {
        return JsonUtils.toJson(featureNames);
    }

    public static FeatureDictionary fromJson(String columnName, String json) {
        return new FeatureDictionary(columnName, JsonUtils.parseStringArray(json));
    }
}
