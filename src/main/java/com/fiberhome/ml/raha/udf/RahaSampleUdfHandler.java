package com.fiberhome.ml.raha.udf;

import java.io.Serializable;

/**
 * 将采样 UDF 请求直接适配到采样业务服务。
 */
@FunctionalInterface
public interface RahaSampleUdfHandler extends Serializable {

    /** 直接执行采样请求并返回终态。 */
    RahaUdfExecutionResult handle(RahaSampleUdfRequest request);
}
