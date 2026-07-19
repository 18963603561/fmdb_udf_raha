package com.fiberhome.ml.raha.annotation.domain;

import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 保存一条用户整行标注及系统确定性展开的直接单元格标签。
 */
public final class RowAnnotation {

    /** 来源标注任务，可在直接导入模式为空。 */
    private final String annotationTaskId;
    /** 采样逻辑行标识。 */
    private final String rowId;
    /** 采样时使用的来源快照标识。 */
    private final String sourceSnapshotId;
    /** 导出时行内容哈希。 */
    private final String rowContentHash;
    /** 零表示正常，一表示异常。 */
    private final int rowLabel;
    /** 模板中实际要求用户检查的可检测字段。 */
    private final Set<String> reviewedColumns;
    /** 用户声明的异常字段。 */
    private final Set<String> errorColumns;
    /** 可选用户说明。 */
    private final String comment;
    /** 系统展开的直接单元格标签。 */
    private final List<CellLabel> cellLabels;

    public RowAnnotation(String annotationTaskId,
                         String rowId,
                         String sourceSnapshotId,
                         String rowContentHash,
                         int rowLabel,
                         Set<String> reviewedColumns,
                         Set<String> errorColumns,
                         String comment,
                         List<CellLabel> cellLabels) {
        this.annotationTaskId = annotationTaskId;
        this.rowId = ValueUtils.requireNotBlank(rowId, "标注逻辑行标识");
        this.sourceSnapshotId = ValueUtils.requireNotBlank(
                sourceSnapshotId, "标注来源快照标识");
        this.rowContentHash = ValueUtils.requireNotBlank(
                rowContentHash, "标注行内容哈希");
        if ((rowLabel != 0 && rowLabel != 1)
                || reviewedColumns == null || reviewedColumns.isEmpty()
                || errorColumns == null || cellLabels == null
                || cellLabels.size() != reviewedColumns.size()) {
            throw new IllegalArgumentException("整行标注和字段标签参数非法");
        }
        if (rowLabel == 0 && !errorColumns.isEmpty()) {
            throw new IllegalArgumentException("正常行不能包含异常字段");
        }
        if (rowLabel == 1 && errorColumns.isEmpty()) {
            throw new IllegalArgumentException("异常行必须包含异常字段");
        }
        if (!reviewedColumns.containsAll(errorColumns)) {
            throw new IllegalArgumentException("异常字段必须属于已检查字段");
        }
        this.rowLabel = rowLabel;
        this.reviewedColumns = Collections.unmodifiableSet(
                new LinkedHashSet<String>(reviewedColumns));
        this.errorColumns = Collections.unmodifiableSet(
                new LinkedHashSet<String>(errorColumns));
        this.comment = comment;
        this.cellLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(cellLabels));
    }

    public String getAnnotationTaskId() { return annotationTaskId; }
    public String getRowId() { return rowId; }
    public String getSourceSnapshotId() { return sourceSnapshotId; }
    public String getRowContentHash() { return rowContentHash; }
    public int getRowLabel() { return rowLabel; }
    public Set<String> getReviewedColumns() { return reviewedColumns; }
    public Set<String> getErrorColumns() { return errorColumns; }
    public String getComment() { return comment; }
    public List<CellLabel> getCellLabels() { return cellLabels; }
}
