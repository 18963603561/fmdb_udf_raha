package com.fiberhome.ml.raha.udf;

import java.io.Serializable;

/**
 * 可序列化的 UDF 提交代理，在实际调用时解析当前进程运行时提交器。
 */
final class RuntimeRahaUdfJobSubmitter
        implements RahaUdfJobSubmitter, Serializable {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;

    @Override
    public RahaUdfSubmissionResult submit(RahaUdfRequest request) {
        return RahaUdfRuntime.requireSubmitter().submit(request);
    }
}
