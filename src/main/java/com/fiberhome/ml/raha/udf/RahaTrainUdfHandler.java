package com.fiberhome.ml.raha.udf;

import java.io.Serializable;

/**
 * 将训练 UDF 请求直接适配到训练业务服务。
 */
@FunctionalInterface
public interface RahaTrainUdfHandler extends Serializable {

    /** 直接执行训练请求并返回终态。 */
    RahaUdfExecutionResult handle(RahaTrainUdfRequest request);
}
