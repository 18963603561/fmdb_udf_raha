package com.fiberhome.ml.raha.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 同步检测请求，检测范围默认使用模型集合全部字段。
 */
public final class DetectRequest {

    /** 待检测输入引用。 */
    private final String inputReference;
    /** 不可变模型集合版本。 */
    private final String modelSetVersion;
    /** 可选来源类型。 */
    private final String sourceType;
    /** 可选行键覆盖。 */
    private final List<String> rowKeyColumns;
    /** 可选快照标识。 */
    private final String snapshotId;
    /** 可选检测目标字段。 */
    private final List<String> targetColumns;
    /** 是否只保存疑似错误。 */
    private final boolean errorsOnly;

    public DetectRequest(String inputReference, String modelSetVersion, String sourceType,
                         List<String> rowKeyColumns, String snapshotId,
                         List<String> targetColumns, boolean errorsOnly) {
        this.inputReference = inputReference;
        this.modelSetVersion = modelSetVersion;
        this.sourceType = sourceType;
        this.rowKeyColumns = rowKeyColumns == null ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(rowKeyColumns));
        this.snapshotId = snapshotId;
        this.targetColumns = targetColumns == null ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(targetColumns));
        this.errorsOnly = errorsOnly;
    }

    public String getInputReference() { return inputReference; }
    public String getModelSetVersion() { return modelSetVersion; }
    public String getSourceType() { return sourceType; }
    public List<String> getRowKeyColumns() { return rowKeyColumns; }
    public String getSnapshotId() { return snapshotId; }
    public List<String> getTargetColumns() { return targetColumns; }
    public boolean isErrorsOnly() { return errorsOnly; }
}
