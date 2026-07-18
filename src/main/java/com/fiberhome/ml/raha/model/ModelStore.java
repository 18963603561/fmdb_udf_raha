package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.train.TrainingExample;

import java.util.List;
import java.util.Optional;

/**
 * 模型集合、列模型和训练样本的最小持久化端口。
 */
public interface ModelStore {

    Optional<RahaModelSet> findModelSet(String modelSetVersion);

    List<RahaColumnModel> loadColumnModels(String modelSetVersion);

    List<TrainingExample> loadTrainingExamples(String modelSetVersion);

    void save(RahaModelSet modelSet, List<RahaColumnModel> models,
              List<TrainingExample> examples);
}
