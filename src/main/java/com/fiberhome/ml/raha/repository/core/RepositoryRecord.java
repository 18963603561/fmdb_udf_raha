package com.fiberhome.ml.raha.repository.core;

/**
 * 保存统一仓储主键、结果版本、业务对象和更新时间。
 *
 * @param <T> 业务对象类型
 */
public final class RepositoryRecord<T> {

    /** 仓储记录主键。 */
    private final RepositoryKey key;
    /** 中间结果版本。 */
    private final ArtifactVersion version;
    /** 业务对象。 */
    private final T payload;
    /** 记录更新时间。 */
    private final long updatedAt;

    public RepositoryRecord(RepositoryKey key, ArtifactVersion version, T payload, long updatedAt) {
        if (key == null || version == null || payload == null) {
            throw new IllegalArgumentException("仓储主键、结果版本和业务对象不能为空");
        }
        if (updatedAt <= 0L) {
            throw new IllegalArgumentException("仓储更新时间必须大于 0");
        }
        this.key = key;
        this.version = version;
        this.payload = payload;
        this.updatedAt = updatedAt;
    }

    public RepositoryKey getKey() {
        return key;
    }

    public ArtifactVersion getVersion() {
        return version;
    }

    public T getPayload() {
        return payload;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }
}

