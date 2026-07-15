package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Objects;

/**
 * 描述一个中间结果所依赖的配置、快照、阶段和尝试版本。
 */
public final class ArtifactVersion {

    /** 完整任务配置版本。 */
    private final String configVersion;
    /** 输入数据快照标识。 */
    private final String snapshotId;
    /** 生成结果的阶段标识。 */
    private final String stageId;
    /** 阶段尝试序号。 */
    private final int attemptId;

    public ArtifactVersion(String configVersion, String snapshotId, String stageId, int attemptId) {
        this.configVersion = ValueUtils.requireNotBlank(configVersion, "配置版本");
        this.snapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
        if (attemptId < 0) {
            throw new IllegalArgumentException("阶段尝试序号不能小于 0");
        }
        this.attemptId = attemptId;
    }

    public String getConfigVersion() {
        return configVersion;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getStageId() {
        return stageId;
    }

    public int getAttemptId() {
        return attemptId;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof ArtifactVersion)) {
            return false;
        }
        ArtifactVersion that = (ArtifactVersion) object;
        return attemptId == that.attemptId
                && configVersion.equals(that.configVersion)
                && snapshotId.equals(that.snapshotId)
                && stageId.equals(that.stageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configVersion, snapshotId, stageId, attemptId);
    }
}

