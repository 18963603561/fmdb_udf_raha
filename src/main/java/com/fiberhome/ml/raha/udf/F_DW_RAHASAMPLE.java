package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;

/**
 * `F_DW_RAHASAMPLE` 表级入口，异步提交聚类覆盖采样任务。
 */
public final class F_DW_RAHASAMPLE extends AbstractRahaTableUdf {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;

    public F_DW_RAHASAMPLE() {
        this(new RuntimeRahaUdfJobSubmitter());
    }

    public F_DW_RAHASAMPLE(RahaUdfJobSubmitter submitter) {
        super(RahaTaskType.SAMPLE, new RahaUdfRequestParser(), submitter);
    }
}
