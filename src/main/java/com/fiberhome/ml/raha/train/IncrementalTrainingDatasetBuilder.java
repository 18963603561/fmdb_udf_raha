package com.fiberhome.ml.raha.train;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 合并父模型训练样本和新增样本，新样本覆盖同一行列父样本。
 */
public final class IncrementalTrainingDatasetBuilder {

    public List<TrainingExample> merge(List<TrainingExample> parent,
                                       List<TrainingExample> current) {
        Map<String, TrainingExample> merged = new LinkedHashMap<String, TrainingExample>();
        for (TrainingExample example : parent) {
            merged.put(key(example), example);
        }
        for (TrainingExample example : current) {
            merged.put(key(example), example);
        }
        return new ArrayList<TrainingExample>(merged.values());
    }

    private static String key(TrainingExample example) {
        return example.getColumnName() + '|' + example.getRowId();
    }
}
