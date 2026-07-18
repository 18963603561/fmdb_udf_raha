package com.fiberhome.ml.raha.feature.assembly;

import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存按列冻结的特征字典、单元格稀疏向量和组装指标。
 */
public final class FeatureAssemblyResult {

    /** 按字段索引的特征字典。 */
    private final Map<String, FeatureDictionary> dictionaries;
    /** 全部单元格稀疏向量。 */
    private final List<SparseFeatureRow> rows;
    /** 特征组装指标。 */
    private final FeatureAssemblyMetrics metrics;

    public FeatureAssemblyResult(Map<String, FeatureDictionary> dictionaries,
                                 List<SparseFeatureRow> rows,
                                 FeatureAssemblyMetrics metrics) {
        if (dictionaries == null || rows == null || metrics == null) {
            throw new IllegalArgumentException("特征字典、向量和指标不能为空");
        }
        this.dictionaries = Collections.unmodifiableMap(
                new LinkedHashMap<String, FeatureDictionary>(dictionaries));
        this.rows = Collections.unmodifiableList(new ArrayList<SparseFeatureRow>(rows));
        this.metrics = metrics;
    }

    public Map<String, FeatureDictionary> getDictionaries() {
        return dictionaries;
    }

    public List<SparseFeatureRow> getRows() {
        return rows;
    }

    public FeatureAssemblyMetrics getMetrics() {
        return metrics;
    }

    public List<SparseFeatureRow> getRowsByColumn(String columnName) {
        List<SparseFeatureRow> matches = new ArrayList<SparseFeatureRow>();
        for (SparseFeatureRow row : rows) {
            if (row.getColumnName().equals(columnName)) {
                matches.add(row);
            }
        }
        return Collections.unmodifiableList(matches);
    }
}
