package com.fiberhome.ml.raha.sampling.domain;

import com.fiberhome.ml.raha.data.loader.RowFingerprintAlgorithm;
import com.fiberhome.ml.raha.data.loader.RowIdentityMode;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存一条不可变 c1 采样逻辑行及其行身份、模式和标注任务上下文。
 */
public final class SampleRecord {

    /** 采样批次标识。 */
    private final String sampleBatchId;
    /** 数据集标识。 */
    private final String datasetId;
    /** 原始输入引用。 */
    private final String inputReference;
    /** 可选数据源版本。 */
    private final String sourceVersion;
    /** 行身份模式。 */
    private final RowIdentityMode rowIdentityMode;
    /** 按配置顺序保存的业务键字段。 */
    private final List<String> rowKeyColumns;
    /** 行内容指纹算法。 */
    private final RowFingerprintAlgorithm fingerprintAlgorithm;
    /** 行规范化协议版本。 */
    private final String fingerprintVersion;
    /** 稳定逻辑行标识。 */
    private final String rowId;
    /** 全部业务字段内容哈希。 */
    private final String rowContentHash;
    /** 业务模式哈希。 */
    private final String schemaHash;
    /** 有序业务字段模式。 */
    private final Map<String, Object> columnSchema;
    /** 可信采样原始行。 */
    private final Map<String, Object> rowData;
    /** 折叠到当前逻辑行的物理行数量。 */
    private final long duplicateCount;
    /** 采样算法版本。 */
    private final String samplingVersion;
    /** 不可变标注任务和采样原因。 */
    private final Map<String, Object> samplingContext;
    /** 采样批次创建时间。 */
    private final long createdAt;
    /** UTC 月分区。 */
    private final String partitionMonth;

    public SampleRecord(String sampleBatchId,
                        String datasetId,
                        String inputReference,
                        String sourceVersion,
                        RowIdentityMode rowIdentityMode,
                        List<String> rowKeyColumns,
                        RowFingerprintAlgorithm fingerprintAlgorithm,
                        String fingerprintVersion,
                        String rowId,
                        String rowContentHash,
                        String schemaHash,
                        Map<String, Object> columnSchema,
                        Map<String, Object> rowData,
                        long duplicateCount,
                        String samplingVersion,
                        Map<String, Object> samplingContext,
                        long createdAt,
                        String partitionMonth) {
        this.sampleBatchId = ValueUtils.requireNotBlank(
                sampleBatchId, "采样批次标识");
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "采样数据集标识");
        this.inputReference = ValueUtils.requireNotBlank(
                inputReference, "采样输入引用");
        this.sourceVersion = sourceVersion;
        if (rowIdentityMode == null || fingerprintAlgorithm == null) {
            throw new IllegalArgumentException("采样行身份模式和算法不能为空");
        }
        this.rowIdentityMode = rowIdentityMode;
        this.rowKeyColumns = immutableList(rowKeyColumns);
        this.fingerprintAlgorithm = fingerprintAlgorithm;
        this.fingerprintVersion = ValueUtils.requireNotBlank(
                fingerprintVersion, "采样行规范版本");
        this.rowId = ValueUtils.requireNotBlank(rowId, "采样逻辑行标识");
        this.rowContentHash = ValueUtils.requireNotBlank(
                rowContentHash, "采样行内容哈希");
        this.schemaHash = ValueUtils.requireNotBlank(schemaHash, "采样模式哈希");
        this.columnSchema = immutableMap(columnSchema, "采样字段模式");
        this.rowData = immutableMap(rowData, "采样原始行");
        if (duplicateCount <= 0L || createdAt <= 0L) {
            throw new IllegalArgumentException("采样重复数量和创建时间必须大于零");
        }
        this.duplicateCount = duplicateCount;
        this.samplingVersion = ValueUtils.requireNotBlank(
                samplingVersion, "采样算法版本");
        this.samplingContext = immutableMap(samplingContext, "采样任务上下文");
        this.createdAt = createdAt;
        this.partitionMonth = ValueUtils.requireNotBlank(
                partitionMonth, "采样月分区");
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public String getDatasetId() { return datasetId; }
    public String getInputReference() { return inputReference; }
    public String getSourceVersion() { return sourceVersion; }
    public RowIdentityMode getRowIdentityMode() { return rowIdentityMode; }
    public List<String> getRowKeyColumns() { return rowKeyColumns; }
    public RowFingerprintAlgorithm getFingerprintAlgorithm() {
        return fingerprintAlgorithm;
    }
    public String getFingerprintVersion() { return fingerprintVersion; }
    public String getRowId() { return rowId; }
    public String getRowContentHash() { return rowContentHash; }
    public String getSchemaHash() { return schemaHash; }
    public Map<String, Object> getColumnSchema() { return columnSchema; }
    public Map<String, Object> getRowData() { return rowData; }
    public long getDuplicateCount() { return duplicateCount; }
    public String getSamplingVersion() { return samplingVersion; }
    public Map<String, Object> getSamplingContext() { return samplingContext; }
    public long getCreatedAt() { return createdAt; }
    public String getPartitionMonth() { return partitionMonth; }

    private static List<String> immutableList(List<String> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<String>(source));
    }

    private static Map<String, Object> immutableMap(Map<String, Object> source,
                                                    String name) {
        if (source == null || source.isEmpty()) {
            throw new IllegalArgumentException(name + "不能为空");
        }
        return Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(source));
    }
}
