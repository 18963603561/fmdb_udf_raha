package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.api.RahaFacade;

/**
 * `F_DW_RAHADETECT` 驱动进程同步检测入口。
 */
public final class F_DW_RAHADETECT extends AbstractRahaTableUdf {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 固定请求解析器。 */
    private final RahaRequestParser parser = new RahaRequestParser();

    public F_DW_RAHADETECT() {
        super("DETECT");
    }

    @Override
    protected String execute(RahaFacade facade, String encodedRequest) {
        return facade.detect(parser.parseDetect(encodedRequest)).toJson();
    }
}
