package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;

/**
 * `F_DW_RAHADETECT` 表级入口，异步提交整表 Raha 检测任务。
 */
public final class F_DW_RAHADETECT extends AbstractRahaTableUdf {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;

    public F_DW_RAHADETECT() {
        this(new RuntimeRahaUdfJobSubmitter());
    }

    public F_DW_RAHADETECT(RahaUdfJobSubmitter submitter) {
        super(RahaTaskType.DETECT, new RahaUdfRequestParser(), submitter);
    }
}
