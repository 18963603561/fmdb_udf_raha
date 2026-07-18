package com.fiberhome.ml.raha.job;

/**
 * 生成异步任务标识，测试可以替换为确定性实现。
 */
public interface RahaIdGenerator {

    String newJobId();
}
