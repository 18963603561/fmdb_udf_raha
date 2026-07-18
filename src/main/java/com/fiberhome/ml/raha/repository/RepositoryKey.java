package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Objects;

/**
 * 统一仓储主键，由命名空间、分区键和记录键组成。
 */
public final class RepositoryKey {

    /** 记录所属业务命名空间。 */
    private final RepositoryNamespace namespace;
    /** 用于批次或任务范围查询的分区键。 */
    private final String partitionKey;
    /** 分区内稳定且唯一的记录键。 */
    private final String recordKey;

    public RepositoryKey(RepositoryNamespace namespace, String partitionKey, String recordKey) {
        if (namespace == null) {
            throw new IllegalArgumentException("仓储命名空间不能为空");
        }
        this.namespace = namespace;
        this.partitionKey = ValueUtils.requireNotBlank(partitionKey, "仓储分区键");
        this.recordKey = ValueUtils.requireNotBlank(recordKey, "仓储记录键");
    }

    public RepositoryNamespace getNamespace() {
        return namespace;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getRecordKey() {
        return recordKey;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RepositoryKey)) {
            return false;
        }
        RepositoryKey that = (RepositoryKey) object;
        return namespace == that.namespace
                && partitionKey.equals(that.partitionKey)
                && recordKey.equals(that.recordKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, partitionKey, recordKey);
    }

    @Override
    public String toString() {
        return namespace + "/" + partitionKey + "/" + recordKey;
    }
}

