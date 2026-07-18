package com.fiberhome.ml.raha.job;

import java.util.UUID;

/**
 * 使用随机 UUID 生成默认异步任务标识。
 */
public final class DefaultRahaIdGenerator implements RahaIdGenerator {

    @Override
    public String newJobId() {
        return "job-" + UUID.randomUUID().toString();
    }
}
