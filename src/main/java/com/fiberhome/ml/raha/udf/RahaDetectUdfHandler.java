package com.fiberhome.ml.raha.udf;

import java.io.Serializable;

/**
 * 将检测 UDF 请求直接适配到检测业务服务。
 */
@FunctionalInterface
public interface RahaDetectUdfHandler extends Serializable {

    /** 直接执行检测请求并返回终态。 */
    RahaUdfExecutionResult handle(RahaDetectUdfRequest request);
}
