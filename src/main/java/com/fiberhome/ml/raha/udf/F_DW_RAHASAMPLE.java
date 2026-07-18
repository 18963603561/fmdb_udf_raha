package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.api.RahaFacade;

/**
 * `F_DW_RAHASAMPLE` 驱动进程同步采样入口。
 */
public final class F_DW_RAHASAMPLE extends AbstractRahaTableUdf {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 固定请求解析器。 */
    private final RahaRequestParser parser = new RahaRequestParser();

    public F_DW_RAHASAMPLE() {
        super("SAMPLE");
    }

    @Override
    protected String execute(RahaFacade facade, String encodedRequest) {
        return facade.sample(parser.parseSample(encodedRequest)).toJson();
    }
}
