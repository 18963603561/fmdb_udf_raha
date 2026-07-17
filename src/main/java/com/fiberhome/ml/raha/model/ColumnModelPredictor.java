package com.fiberhome.ml.raha.model;

/**
 * 列分类器预测扩展接口。
 */
public interface ColumnModelPredictor {

    double predict(RahaColumnModel model, String value);
}
