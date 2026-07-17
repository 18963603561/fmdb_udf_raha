package com.fiberhome.ml.raha.train;

import java.util.List;

/**
 * 完整训练样本快照的最小存取端口。
 */
public interface TrainingExampleStore {

    List<TrainingExample> load(String modelSetVersion);
}
