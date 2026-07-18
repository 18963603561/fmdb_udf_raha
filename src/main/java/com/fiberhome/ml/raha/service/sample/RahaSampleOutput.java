package com.fiberhome.ml.raha.service.sample;

import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.sampling.service.SamplingBatchResult;

/**
 * 保存采样服务的聚类结果和待标注任务结果。
 */
public final class RahaSampleOutput {

    /** 本轮列内聚类结果。 */
    private final ClusteringBatchResult clustering;
    /** 本轮覆盖采样和待标注任务。 */
    private final SamplingBatchResult sampling;

    public RahaSampleOutput(ClusteringBatchResult clustering,
                            SamplingBatchResult sampling) {
        if (clustering == null || sampling == null) {
            throw new IllegalArgumentException("采样服务输出不能为空");
        }
        this.clustering = clustering;
        this.sampling = sampling;
    }

    public ClusteringBatchResult getClustering() { return clustering; }
    public SamplingBatchResult getSampling() { return sampling; }
}
