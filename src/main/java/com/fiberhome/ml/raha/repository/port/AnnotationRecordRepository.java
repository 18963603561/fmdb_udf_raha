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
     * 判断同一采样批次的相同上传文件是否已经成功入库。
     *
     * @param datasetId 数据集标识
     * @param sampleBatchId 采样批次标识
     * @param importFingerprint 上传文件 SHA-256 指纹
     * @return 已存在时返回真
     */
    boolean existsImportFingerprint(String datasetId,
                                    String sampleBatchId,
                                    String importFingerprint);
}
