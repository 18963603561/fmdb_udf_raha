package com.fiberhome.ml.raha.job.id;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 根据任务类型、数据集、输入引用、快照和配置版本生成幂等键。
 */
public final class IdempotencyKeyGenerator {

    public String generate(RahaJobConfig config, String configVersion) {
        if (config == null) {
            throw new IllegalArgumentException("任务配置不能为空");
        }
        String validatedVersion = ValueUtils.requireNotBlank(configVersion, "配置版本");
        String snapshotId = config.getSnapshotId() == null ? "<pending>" : config.getSnapshotId();
        String source = config.getJobType() + "|"
                + config.getDatasetId().length() + ":" + config.getDatasetId() + "|"
                + config.getInputReference().length() + ":" + config.getInputReference() + "|"
                + snapshotId.length() + ":" + snapshotId + "|"
                + validatedVersion;
        return HashUtils.md5Hex(source);
    }
}

