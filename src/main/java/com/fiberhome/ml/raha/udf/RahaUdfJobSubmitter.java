package com.fiberhome.ml.raha.udf;

/**
 * 提交表级 Raha 异步任务并返回持久化结果。
 */
public interface RahaUdfJobSubmitter {

    RahaUdfSubmissionResult submit(RahaUdfRequest request);
}
