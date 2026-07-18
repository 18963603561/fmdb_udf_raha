package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.train.TrainingExample;

import java.util.List;

/**
 * 列分类器训练扩展接口。
 */
public interface ColumnModelTrainer {

    RahaColumnModel train(String modelSetVersion, String datasetId, String columnName,
                          String parentModelVersion, FeatureDictionary dictionary,
                          List<TrainingExample> examples, long createdAt);
}
