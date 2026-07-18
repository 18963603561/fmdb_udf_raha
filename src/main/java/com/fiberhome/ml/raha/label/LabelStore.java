package com.fiberhome.ml.raha.label;

import com.fiberhome.ml.raha.data.CellLabel;

import java.util.List;

/**
 * 人工直接标签的最小持久化端口。
 */
public interface LabelStore {

    List<CellLabel> load(List<String> sampleBatchIds);

    void save(List<CellLabel> labels);
}
