package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;

/**
 * `F_DW_RAHATRAIN` 表级入口，同步执行 Raha 列级模型训练任务。
 */
public final class F_DW_RAHATRAIN extends AbstractRahaTableUdf<RahaTrainUdfRequest> {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 训练请求解析器。 */
    private final RahaUdfRequestParser parser;
    /** 训练直接执行适配器。 */
    private final RahaTrainUdfHandler handler;

    public F_DW_RAHATRAIN(RahaTrainUdfHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("训练 UDF handler 不能为空");
        }
        this.parser = new RahaUdfRequestParser();
        this.handler = handler;
    }

    @Override
    protected RahaTaskType taskType() {
        return RahaTaskType.TRAIN;
    }

    @Override
    protected RahaTrainUdfRequest parse(String encodedRequest) {
        return parser.parseTrain(encodedRequest);
    }

    @Override
    protected RahaUdfCommonFields commonFields(RahaTrainUdfRequest request) {
        return request.getCommonFields();
    }

    @Override
    protected String canonicalConfiguration(RahaTrainUdfRequest request) {
        return request.toCanonicalConfiguration();
    }

    @Override
    protected RahaUdfExecutionResult handle(RahaTrainUdfRequest request) {
        return handler.handle(request);
    }
}
