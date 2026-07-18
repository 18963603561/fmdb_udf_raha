package com.fiberhome.ml.raha.model.training;

/**
 * 单列分类模型训练统一接口。
 */
public interface ColumnModelTrainer {

    ColumnModelTrainingResult train(ColumnModelTrainingRequest request);
}
