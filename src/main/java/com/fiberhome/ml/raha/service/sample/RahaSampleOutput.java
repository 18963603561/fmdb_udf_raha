package com.fiberhome.ml.raha.service.sample;

import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.sampling.service.SamplingBatchResult;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;

/**
 * 保存采样服务的聚类结果和待标注任务结果。
 */
public final class RahaSampleOutput {

    /** 本轮列内聚类结果。 */
    private final ClusteringBatchResult clustering;
    /** 本轮覆盖采样和待标注任务。 */
    private final SamplingBatchResult sampling;
    /** 已经物理持久化并回读验证的 c1 批次。 */
    private final SampleBatch sampleBatch;

    public RahaSampleOutput(ClusteringBatchResult clustering,
                            SamplingBatchResult sampling) {
        this(clustering, sampling, null);
    }

    public RahaSampleOutput(ClusteringBatchResult clustering,
                            SamplingBatchResult sampling,
                            SampleBatch sampleBatch) {
        if (clustering == null || sampling == null) {
            throw new IllegalArgumentException("采样服务输出不能为空");
        }
        this.clustering = clustering;
        this.sampling = sampling;
        this.sampleBatch = sampleBatch;
    }

    public ClusteringBatchResult getClustering() { return clustering; }
    public SamplingBatchResult getSampling() { return sampling; }
    public SampleBatch getSampleBatch() { return sampleBatch; }

    public RahaSampleOutput withSampleBatch(SampleBatch batch) {
        if (batch == null) {
            throw new IllegalArgumentException("c1 采样批次不能为空");
        }
        return new RahaSampleOutput(clustering, sampling, batch);
    }
}
