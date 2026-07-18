package com.fiberhome.ml.raha.repository;

import java.util.List;
import java.util.Optional;

/**
 * Raha 中间结果统一仓储接口，FMDB 适配层后续实现同一契约。
 */
public interface RahaRepository {

    /**
     * 保存版本化记录，相同主键和版本重复写入时必须去重。
     *
     * @param record 待保存记录
     * @return 保存结果
     */
    SaveOutcome save(RepositoryRecord<?> record);

    /**
     * 根据主键读取并校验业务对象类型。
     *
     * @param key 仓储主键
     * @param payloadType 业务对象类型
     * @param <T> 业务对象类型
     * @return 可选记录
     */
    <T> Optional<RepositoryRecord<T>> find(RepositoryKey key, Class<T> payloadType);

    /**
     * 查询一个命名空间和分区下的全部记录。
     *
     * @param namespace 命名空间
     * @param partitionKey 分区键
     * @param payloadType 业务对象类型
     * @param <T> 业务对象类型
     * @return 按记录键排序的记录列表
     */
    <T> List<RepositoryRecord<T>> findByPartition(RepositoryNamespace namespace,
                                                  String partitionKey,
                                                  Class<T> payloadType);

    /**
     * 在仓储事务边界内执行一组操作。
     *
     * @param transaction 事务回调
     */
    void executeInTransaction(RepositoryTransaction transaction);

    /**
     * 返回当前有效记录数量，主要用于开发期验证和监控。
     *
     * @return 有效记录数量
     */
    int size();
}

