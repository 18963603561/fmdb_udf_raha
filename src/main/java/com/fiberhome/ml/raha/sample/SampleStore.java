package com.fiberhome.ml.raha.sample;

import java.util.List;
import java.util.Optional;

/**
 * 采样批次和元组的最小持久化端口。
 */
public interface SampleStore {

    Optional<SampleBatch> findBatch(String sampleBatchId);

    List<SampleBatch> loadBatches(List<String> sampleBatchIds);

    List<SampleTuple> loadTuples(List<String> sampleBatchIds);

    void save(SampleBatch batch, List<SampleTuple> tuples);
}
