package com.fiberhome.ml.raha.sampling.service;

import com.fiberhome.ml.raha.sampling.domain.SampleBatch;

/**
 * 返回 c1 物化批次、实际新增数量和可验证物理位置。
 */
public final class SampleMaterializationResult {

    /** 已生成的采样批次。 */
    private final SampleBatch batch;
    /** 本次物理新增记录数量。 */
    private final long writtenCount;
    /** 是否完成物理写入并回读验证。 */
    private final boolean persisted;
    /** 可供标注和训练读取的物理批次位置。 */
    private final String resultLocation;

    public SampleMaterializationResult(SampleBatch batch,
                                       long writtenCount,
                                       boolean persisted,
                                       String resultLocation) {
        if (batch == null || writtenCount < 0L
                || (persisted && (resultLocation == null
                || resultLocation.trim().isEmpty()))) {
            throw new IllegalArgumentException("采样物化结果参数非法");
        }
        this.batch = batch;
        this.writtenCount = writtenCount;
        this.persisted = persisted;
        this.resultLocation = resultLocation;
    }

    public SampleBatch getBatch() { return batch; }
    public long getWrittenCount() { return writtenCount; }
    public boolean isPersisted() { return persisted; }
    public String getResultLocation() { return resultLocation; }
}
