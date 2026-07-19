package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.ModelPersistenceContext;

/**
 * 保存和加载可移植列级模型参数文件。
 */
public interface ColumnModelStore {

    String save(ColumnModelArtifact artifact, ModelPersistenceContext context);

    ColumnModelArtifact load(String modelPath);
}
