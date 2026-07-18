package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;

/**
 * `F_DW_RAHADETECT` 表级入口，同步执行整表 Raha 检测任务。
 */
public final class F_DW_RAHADETECT extends AbstractRahaTableUdf<RahaDetectUdfRequest> {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 检测请求解析器。 */
    private final RahaUdfRequestParser parser;
    /** 检测直接执行适配器。 */
    private final RahaDetectUdfHandler handler;

    public F_DW_RAHADETECT(RahaDetectUdfHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("检测 UDF handler 不能为空");
        }
        this.parser = new RahaUdfRequestParser();
        this.handler = handler;
    }

    @Override
    protected RahaTaskType taskType() {
        return RahaTaskType.DETECT;
    }

    @Override
    protected RahaDetectUdfRequest parse(String encodedRequest) {
        return parser.parseDetect(encodedRequest);
    }

    @Override
    protected RahaUdfCommonFields commonFields(RahaDetectUdfRequest request) {
        return request.getCommonFields();
    }

    @Override
    protected String canonicalConfiguration(RahaDetectUdfRequest request) {
        return request.toCanonicalConfiguration();
    }

    @Override
    protected RahaUdfExecutionResult handle(RahaDetectUdfRequest request) {
        return handler.handle(request);
    }
}
