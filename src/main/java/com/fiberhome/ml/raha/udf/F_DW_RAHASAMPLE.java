package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;

/**
 * `F_DW_RAHASAMPLE` 表级入口，同步执行聚类覆盖采样任务。
 */
public final class F_DW_RAHASAMPLE extends AbstractRahaTableUdf<RahaSampleUdfRequest> {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 采样请求解析器。 */
    private final RahaUdfRequestParser parser;
    /** 采样直接执行适配器。 */
    private final RahaSampleUdfHandler handler;

    public F_DW_RAHASAMPLE(RahaSampleUdfHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("采样 UDF handler 不能为空");
        }
        this.parser = new RahaUdfRequestParser();
        this.handler = handler;
    }

    @Override
    protected RahaTaskType taskType() {
        return RahaTaskType.SAMPLE;
    }

    @Override
    protected RahaSampleUdfRequest parse(String encodedRequest) {
        return parser.parseSample(encodedRequest);
    }

    @Override
    protected RahaUdfCommonFields commonFields(RahaSampleUdfRequest request) {
        return request.getCommonFields();
    }

    @Override
    protected String canonicalConfiguration(RahaSampleUdfRequest request) {
        return request.toCanonicalConfiguration();
    }

    @Override
    protected RahaUdfExecutionResult handle(RahaSampleUdfRequest request) {
        return handler.handle(request);
    }
}
