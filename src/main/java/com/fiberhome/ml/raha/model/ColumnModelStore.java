package com.fiberhome.ml.raha.model;

/**
 * 保存和加载可移植列级模型参数文件。
 */
public interface ColumnModelStore {

    String save(ColumnModelArtifact artifact);

    ColumnModelArtifact load(String modelPath);
}
