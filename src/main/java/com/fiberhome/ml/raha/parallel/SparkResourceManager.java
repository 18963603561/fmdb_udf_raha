package com.fiberhome.ml.raha.parallel;

import com.fiberhome.ml.raha.config.ResourceConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

/**
 * 根据资源配置控制小对象广播和数据集缓存，超过估算上限时拒绝操作。
 */
public final class SparkResourceManager {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SparkResourceManager.class);

    /**
     * 仅在估算大小不超过阈值时创建 Spark 广播变量。
     *
     * @param sparkSession Spark 会话
     * @param value 可序列化小对象
     * @param estimatedBytes 估算字节数
     * @param config 资源限制配置
     * @param <T> 广播对象类型
     * @return 已创建广播变量，超过上限时为空
     */
    public <T extends Serializable> Optional<Broadcast<T>> broadcastIfAllowed(
            SparkSession sparkSession,
            T value,
            long estimatedBytes,
            ResourceConfig config) {
        validate(sparkSession, estimatedBytes, config);
        if (value == null) {
            throw new IllegalArgumentException("广播对象不能为空");
        }
        if (estimatedBytes > config.getBroadcastThresholdBytes()) {
            LOGGER.warn("对象超过广播上限，不创建广播变量，estimatedBytes={}，thresholdBytes={}",
                    estimatedBytes, config.getBroadcastThresholdBytes());
            return Optional.empty();
        }
        LOGGER.info("开始创建受控广播变量，estimatedBytes={}，thresholdBytes={}",
                estimatedBytes, config.getBroadcastThresholdBytes());
        Broadcast<T> broadcast = JavaSparkContext.fromSparkContext(
                sparkSession.sparkContext()).broadcast(value);
        LOGGER.info("受控广播变量创建完成，broadcastId={}", broadcast.id());
        return Optional.of(broadcast);
    }

    /**
     * 仅在估算大小不超过缓存阈值时按配置持久化数据集。
     *
     * @param dataset 待缓存数据集
     * @param estimatedBytes 估算字节数
     * @param config 资源限制配置
     * @return 实际执行缓存返回真
     */
    public boolean persistIfAllowed(Dataset<?> dataset,
                                    long estimatedBytes,
                                    ResourceConfig config) {
        if (dataset == null) {
            throw new IllegalArgumentException("待缓存数据集不能为空");
        }
        validate(dataset.sparkSession(), estimatedBytes, config);
        if (estimatedBytes > config.getCacheThresholdBytes()) {
            LOGGER.warn("数据集超过缓存上限，不执行持久化，estimatedBytes={}，thresholdBytes={}",
                    estimatedBytes, config.getCacheThresholdBytes());
            return false;
        }
        StorageLevel storageLevel = storageLevel(config.getCacheStorageLevel());
        LOGGER.info("开始受控缓存数据集，estimatedBytes={}，storageLevel={}",
                estimatedBytes, config.getCacheStorageLevel());
        dataset.persist(storageLevel);
        return true;
    }

    private static void validate(SparkSession sparkSession,
                                 long estimatedBytes,
                                 ResourceConfig config) {
        if (sparkSession == null || config == null || estimatedBytes < 0L) {
            throw new IllegalArgumentException("Spark 会话、估算大小和资源配置必须有效");
        }
    }

    private static StorageLevel storageLevel(String value) {
        if ("MEMORY_ONLY".equals(value)) {
            return StorageLevel.MEMORY_ONLY();
        }
        if ("MEMORY_AND_DISK".equals(value)) {
            return StorageLevel.MEMORY_AND_DISK();
        }
        if ("DISK_ONLY".equals(value)) {
            return StorageLevel.DISK_ONLY();
        }
        throw new IllegalArgumentException("不支持的 Spark 缓存级别：" + value);
    }
}
