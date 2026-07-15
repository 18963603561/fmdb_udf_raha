package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;

/**
 * `F_DW_RAHATRAIN` 表级入口，异步提交 Raha 列级模型训练任务。
 */
public final class F_DW_RAHATRAIN extends AbstractRahaTableUdf {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;

    public F_DW_RAHATRAIN() {
        this(new RuntimeRahaUdfJobSubmitter());
    }

    public F_DW_RAHATRAIN(RahaUdfJobSubmitter submitter) {
        super(RahaTaskType.TRAIN, new RahaUdfRequestParser(), submitter);
    }
}
