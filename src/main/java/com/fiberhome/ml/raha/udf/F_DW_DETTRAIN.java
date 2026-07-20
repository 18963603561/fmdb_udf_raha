package com.fiberhome.ml.raha.udf;

import java.util.List;

/**
 * Detection Train，根据采样批次和人工标注训练检测模型。
 */
public final class F_DW_DETTRAIN extends AbstractRahaGenericUdf {

    @Override
    protected String functionName() {
        return "F_DW_DETTRAIN";
    }

    @Override
    protected List<RahaUdfField> fields() {
        return RahaUdfFields.TRAIN;
    }

    @Override
    protected RahaUdfRows doEvaluate(String argument,
                                     RahaDetectionUdfService service) {
        return service.train(argument);
    }
}
