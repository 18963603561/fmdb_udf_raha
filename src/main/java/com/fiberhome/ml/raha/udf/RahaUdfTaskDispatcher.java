package com.fiberhome.ml.raha.udf;

/**
 * 将已经通过 UDF 校验并持久化的任务请求分发到核心服务。
 */
@FunctionalInterface
public interface RahaUdfTaskDispatcher {

    /**
     * 执行一个异步 UDF 任务。
     *
     * @param request 完整任务请求
     * @return 不包含原始数据的执行摘要
     */
    String dispatch(RahaUdfRequest request);
}
