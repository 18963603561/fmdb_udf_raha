package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import java.util.Optional;

/**
 * 定义不可变标注批次的 FMDB 追加、批次查询和最新修订选择边界。
 */
public interface AnnotationRecordRepository {

    boolean isPersistenceEnabled();

    long saveAll(AnnotationBatch batch);

    Optional<AnnotationBatch> find(String datasetId,
                                   String partitionMonth,
                                   String annotationBatchId);

    Optional<AnnotationBatch> findLatestForSample(String datasetId,
                                                  String partitionMonth,
                                                  String sampleBatchId);

    /**
     * 按全局采样批次标识选择最新可用于训练的标注批次。
     *
     * <p>默认实现用于兼容不支持全局索引的测试仓储；生产 FMDB 适配器必须覆盖该方法。</p>
     *
     * @param sampleBatchId 全局唯一采样批次标识
     * @param allowPartial 是否允许部分成功导入批次
     * @return 最新符合状态规则的标注批次
     */
    default Optional<AnnotationBatch> findLatestTrainableForSample(
            String sampleBatchId,
            boolean allowPartial) {
        return Optional.empty();
    }

    /**
     * 判断同一采样批次的相同上传文件是否已经成功入库。
     *
     * @param datasetId 数据集标识
     * @param sampleBatchId 采样批次标识
     * @param importFingerprint 上传文件 MD5 指纹
     * @return 已存在时返回真
     */
    boolean existsImportFingerprint(String datasetId,
                                    String sampleBatchId,
                                    String importFingerprint);
}
