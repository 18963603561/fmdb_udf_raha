package com.fiberhome.ml.raha.udf;

import java.io.Serializable;

/**
 * 可序列化的 UDF 提交代理，在实际调用时解析当前进程运行时提交器。
 */
final class RuntimeRahaUdfJobSubmitter
        implements RahaUdfJobSubmitter, Serializable {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 独立注册模式下随 UDF 序列化的文件提交器。 */
    private final RahaUdfJobSubmitter standaloneSubmitter;

    RuntimeRahaUdfJobSubmitter() {
        this.standaloneSubmitter = FileRahaUdfJobSubmitter.fromConfiguration();
    }

    @Override
    public RahaUdfSubmissionResult submit(RahaUdfRequest request) {
        RahaUdfJobSubmitter configured = RahaUdfRuntime.currentSubmitter();
        if (configured != null) {
            return configured.submit(request);
        }
        if (standaloneSubmitter != null) {
            return standaloneSubmitter.submit(request);
        }
        return RahaUdfRuntime.requireSubmitter().submit(request);
    }
}
