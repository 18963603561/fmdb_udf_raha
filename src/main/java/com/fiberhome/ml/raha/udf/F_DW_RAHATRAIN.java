package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.api.RahaFacade;

/**
 * `F_DW_RAHATRAIN` 驱动进程同步训练入口。
 */
public final class F_DW_RAHATRAIN extends AbstractRahaTableUdf {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 固定请求解析器。 */
    private final RahaRequestParser parser = new RahaRequestParser();

    public F_DW_RAHATRAIN() {
        super("TRAIN");
    }

    @Override
    protected String execute(RahaFacade facade, String encodedRequest) {
        return facade.train(parser.parseTrain(encodedRequest)).toJson();
    }
}
