package com.fiberhome.ml.raha.udf;

import java.util.List;

/**
 * Detection Collect，采集检测样本并生成待标注 Excel ZIP。
 */
public final class F_DW_DETCOLLECT extends AbstractRahaGenericUdf {

    @Override
    protected String functionName() {
        return "F_DW_DETCOLLECT";
    }

    @Override
    protected List<RahaUdfField> fields() {
        return RahaUdfFields.COLLECT;
    }

    @Override
    protected RahaUdfRows doEvaluate(String argument,
                                     RahaDetectionUdfService service) {
        return service.collect(argument);
    }
}
