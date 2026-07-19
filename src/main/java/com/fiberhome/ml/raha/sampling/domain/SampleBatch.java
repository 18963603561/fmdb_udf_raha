package com.fiberhome.ml.raha.sampling.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 保存一次可独立标注和训练读取的不可变 c1 采样批次。
 */
public final class SampleBatch {

    /** 采样批次标识。 */
    private final String sampleBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 采样来源快照。 */
    private final String snapshotId;
    /** 可选数据源版本。 */
    private final String sourceVersion;
    /** 采样算法版本。 */
    private final String samplingVersion;
    /** 批次创建时间。 */
    private final long createdAt;
    /** UTC 月分区。 */
    private final String partitionMonth;
    /** 批次内不可变采样逻辑行。 */
    private final List<SampleRecord> records;

    public SampleBatch(String sampleBatchId,
                       String datasetId,
                       String snapshotId,
                       String sourceVersion,
                       String samplingVersion,
                       long createdAt,
                       String partitionMonth,
                       List<SampleRecord> records) {
        this.sampleBatchId = ValueUtils.requireNotBlank(
                sampleBatchId, "采样批次标识");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "采样数据集标识");
        this.snapshotId = ValueUtils.requireNotBlank(snapshotId, "采样快照标识");
        this.sourceVersion = sourceVersion;
        this.samplingVersion = ValueUtils.requireNotBlank(
                samplingVersion, "采样算法版本");
        this.partitionMonth = ValueUtils.requireNotBlank(
                partitionMonth, "采样月分区");
        if (createdAt <= 0L || records == null || records.isEmpty()) {
            throw new IllegalArgumentException("采样批次时间和记录必须有效");
        }
        this.createdAt = createdAt;
        Set<String> rowIds = new LinkedHashSet<String>();
        List<SampleRecord> copy = new ArrayList<SampleRecord>(records.size());
        for (SampleRecord record : records) {
            if (record == null
                    || !sampleBatchId.equals(record.getSampleBatchId())
                    || !datasetId.equals(record.getDatasetId())
                    || !samplingVersion.equals(record.getSamplingVersion())
                    || !partitionMonth.equals(record.getPartitionMonth())
                    || !rowIds.add(record.getRowId())) {
                throw new IllegalArgumentException("采样批次记录归属或行标识不一致");
            }
            copy.add(record);
        }
        this.records = Collections.unmodifiableList(copy);
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public String getDatasetId() { return datasetId; }
    public String getSnapshotId() { return snapshotId; }
    public String getSourceVersion() { return sourceVersion; }
    public String getSamplingVersion() { return samplingVersion; }
    public long getCreatedAt() { return createdAt; }
    public String getPartitionMonth() { return partitionMonth; }
    public List<SampleRecord> getRecords() { return records; }
}
