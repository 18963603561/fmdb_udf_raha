package com.fiberhome.ml.raha.udf;

import java.util.List;

/**
 * Detection Run，使用模型集合版本执行数据错误检测。
 */
public final class F_DW_DETRUN extends AbstractRahaGenericUdf {

    @Override
    protected String functionName() {
        return "F_DW_DETRUN";
    }

    @Override
    protected List<RahaUdfField> fields() {
        return RahaUdfFields.DETECT;
    }

    @Override
    protected RahaUdfRows doEvaluate(String argument,
                                     RahaDetectionUdfService service) {
        return service.detect(argument);
    }
}
