package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.core.RepositoryKey;
import com.fiberhome.ml.raha.repository.core.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.core.RepositoryRecord;
import com.fiberhome.ml.raha.repository.core.RepositoryTransaction;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 开发和测试使用的内存仓储，实现幂等写入、版本更新和事务回滚。
 */
public final class InMemoryRahaRepository implements RahaRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryRahaRepository.class);
    /** 当前有效记录，主键唯一。 */
    private final Map<RepositoryKey, RepositoryRecord<?>> records =
            new LinkedHashMap<RepositoryKey, RepositoryRecord<?>>();

    @Override
    public synchronized SaveOutcome save(RepositoryRecord<?> record) {
        if (record == null) {
            throw new IllegalArgumentException("仓储记录不能为空");
        }
        RepositoryRecord<?> existing = records.get(record.getKey());
        if (existing == null) {
            records.put(record.getKey(), record);
            return SaveOutcome.CREATED;
        }
        // 相同主键和版本表示同一批次结果重放，保留已有有效记录并返回去重状态。
        if (existing.getVersion().equals(record.getVersion())) {
            records.put(record.getKey(), record);
            return SaveOutcome.UNCHANGED;
        }
        records.put(record.getKey(), record);
        return SaveOutcome.UPDATED;
    }

    @Override
    public synchronized <T> Optional<RepositoryRecord<T>> find(RepositoryKey key,
                                                                Class<T> payloadType) {
        if (key == null || payloadType == null) {
            throw new IllegalArgumentException("仓储主键和业务对象类型不能为空");
        }
        RepositoryRecord<?> record = records.get(key);
        if (record == null) {
            return Optional.empty();
        }
        if (!payloadType.isInstance(record.getPayload())) {
            throw new IllegalStateException("仓储记录类型与请求类型不一致：" + key);
        }
        return Optional.of(castRecord(record, payloadType));
    }

    @Override
    public synchronized <T> List<RepositoryRecord<T>> findByPartition(
            RepositoryNamespace namespace,
            String partitionKey,
            Class<T> payloadType) {
        if (namespace == null || payloadType == null) {
            throw new IllegalArgumentException("仓储命名空间和业务对象类型不能为空");
        }
        String validatedPartitionKey = ValueUtils.requireNotBlank(partitionKey, "仓储分区键");
        List<RepositoryRecord<T>> matches = new ArrayList<RepositoryRecord<T>>();
        for (RepositoryRecord<?> record : records.values()) {
            RepositoryKey key = record.getKey();
            if (key.getNamespace() == namespace
                    && key.getPartitionKey().equals(validatedPartitionKey)) {
                if (!payloadType.isInstance(record.getPayload())) {
                    throw new IllegalStateException("仓储分区中存在不匹配的业务对象类型：" + key);
                }
                matches.add(castRecord(record, payloadType));
            }
        }
        Collections.sort(matches, new Comparator<RepositoryRecord<T>>() {
            @Override
            public int compare(RepositoryRecord<T> first, RepositoryRecord<T> second) {
                return first.getKey().getRecordKey().compareTo(second.getKey().getRecordKey());
            }
        });
        return Collections.unmodifiableList(matches);
    }

    @Override
    public synchronized void executeInTransaction(RepositoryTransaction transaction) {
        if (transaction == null) {
            throw new IllegalArgumentException("仓储事务不能为空");
        }
        Map<RepositoryKey, RepositoryRecord<?>> before =
                new LinkedHashMap<RepositoryKey, RepositoryRecord<?>>(records);
        try {
            transaction.execute(this);
        } catch (RuntimeException exception) {
            // 开发期仓储在事务失败时恢复全部主键，便于验证后续 FMDB 事务语义。
            records.clear();
            records.putAll(before);
            LOGGER.error("内存仓储事务执行失败，已回滚全部写入", exception);
            throw exception;
        }
    }

    @Override
    public synchronized int size() {
        return records.size();
    }

    /**
     * 删除指定分区的中间记录，供验收流程在特征落盘后释放大批量命中对象。
     *
     * @param namespace 仓储命名空间
     * @param partitionKey 分区标识
     * @return 删除记录数
     */
    public synchronized int removePartition(RepositoryNamespace namespace,
                                             String partitionKey) {
        if (namespace == null || partitionKey == null || partitionKey.trim().isEmpty()) {
            throw new IllegalArgumentException("仓储命名空间和分区标识不能为空");
        }
        int removed = 0;
        java.util.Iterator<Map.Entry<RepositoryKey, RepositoryRecord<?>>> iterator =
                records.entrySet().iterator();
        while (iterator.hasNext()) {
            RepositoryKey key = iterator.next().getKey();
            if (key.getNamespace() == namespace
                    && partitionKey.equals(key.getPartitionKey())) {
                iterator.remove();
                removed++;
            }
        }
        return removed;
    }

    private static <T> RepositoryRecord<T> castRecord(RepositoryRecord<?> record,
                                                       Class<T> payloadType) {
        return new RepositoryRecord<T>(record.getKey(), record.getVersion(),
                payloadType.cast(record.getPayload()), record.getUpdatedAt());
    }
}
