package com.fiberhome.ml.raha.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 同步采样请求，允许系统生成数据集、来源类型、快照和目标字段默认值。
 */
public final class SampleRequest {

    /** 输入表、SQL 或 CSV 引用。 */
    private final String inputReference;
    /** 可选逻辑数据集标识。 */
    private final String datasetId;
    /** 可选来源类型。 */
    private final String sourceType;
    /** 可选业务键字段。 */
    private final List<String> rowKeyColumns;
    /** 可选快照标识。 */
    private final String snapshotId;
    /** 可选目标字段。 */
    private final List<String> targetColumns;
    /** 可选标注预算，小于等于零表示使用默认值。 */
    private final int labelingBudget;

    public SampleRequest(String inputReference, String datasetId, String sourceType,
                         List<String> rowKeyColumns, String snapshotId,
                         List<String> targetColumns, int labelingBudget) {
        this.inputReference = inputReference;
        this.datasetId = datasetId;
        this.sourceType = sourceType;
        this.rowKeyColumns = immutable(rowKeyColumns);
        this.snapshotId = snapshotId;
        this.targetColumns = immutable(targetColumns);
        this.labelingBudget = labelingBudget;
    }

    private static List<String> immutable(List<String> values) {
        return values == null ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(values));
    }

    public String getInputReference() { return inputReference; }
    public String getDatasetId() { return datasetId; }
    public String getSourceType() { return sourceType; }
    public List<String> getRowKeyColumns() { return rowKeyColumns; }
    public String getSnapshotId() { return snapshotId; }
    public List<String> getTargetColumns() { return targetColumns; }
    public int getLabelingBudget() { return labelingBudget; }
}
