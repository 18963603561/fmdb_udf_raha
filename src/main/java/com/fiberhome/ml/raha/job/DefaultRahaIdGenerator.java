package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Locale;
import java.util.UUID;

/**
 * 默认标识生成器，任务使用随机 UUID，阶段使用任务和尝试序号生成稳定标识。
 */
public final class DefaultRahaIdGenerator implements RahaIdGenerator {

    @Override
    public String newJobId() {
        return "job-" + UUID.randomUUID().toString();
    }

    @Override
    public String newStageId(String jobId, StageType stageType, int attemptId) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (stageType == null || attemptId <= 0) {
            throw new IllegalArgumentException("阶段类型不能为空，尝试序号必须大于 0");
        }
        String source = validatedJobId + "|" + stageType.name() + "|" + attemptId;
        return stageType.name().toLowerCase(Locale.ROOT)
                + "-" + HashUtils.sha256Hex(source).substring(0, 16);
    }
}

