package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import java.util.List;
import java.util.Optional;

/**
 * 定义不可变 c1 采样批次的物理追加和分区裁剪读取边界。
 */
public interface SampleRecordRepository {

    /**
     * 判断采样物理表当前是否允许写入。
     */
    boolean isPersistenceEnabled();

    /**
     * 幂等追加采样批次，已存在业务键不会重复写入。
     *
     * @param batch 完整 c1 批次
     * @return 本次实际新增记录数量
     */
    long saveAll(SampleBatch batch);

    /**
     * 按数据集、月分区和采样批次读取完整 c1。
     */
    Optional<SampleBatch> find(String datasetId,
                               String partitionMonth,
                               String sampleBatchId);

    /**
     * 按全局唯一采样批次标识查询完整批次。
     *
     * <p>默认实现用于兼容不支持全局索引的测试仓储；生产 FMDB 适配器必须覆盖该方法。</p>
     *
     * @param sampleBatchId 全局唯一采样批次标识
     * @return 找到时返回完整批次
     */
    default Optional<SampleBatch> findByBatchId(String sampleBatchId) {
        return Optional.empty();
    }

    /**
     * 使用最小字段投影读取标注展示行，禁止标注热路径扫描整张宽表字段。
     */
    List<SampleAnnotationRow> findForAnnotation(String datasetId,
                                                String partitionMonth,
                                                String sampleBatchId);
}
