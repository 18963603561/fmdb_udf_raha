package com.fiberhome.ml.raha.annotation.service;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 将 Excel 的整行标注按固定规则展开为直接单元格标签。
 */
public final class AnnotationLabelExpander {

    /**
     * 按正常行、异常行和未检查字段规则生成直接人工标签。
     *
     * @param datasetId 数据集标识
     * @param snapshotId 标注时可信 c1 快照标识
     * @param rowId 采样逻辑行标识
     * @param rowLabel 整行标签，零表示正常，一表示异常
     * @param reviewedColumns 本次确认检查的字段，顺序决定输出顺序
     * @param errorColumns 用户确认异常的字段
     * @param annotator 标注人员，可为空
     * @param createdAt 标签创建时间
     * @param labelIdPrefix 标签批次前缀，用于生成稳定标签标识
     * @return 按字段顺序生成的直接标签
     */
    public List<CellLabel> expand(String datasetId,
                                  String snapshotId,
                                  String rowId,
                                  int rowLabel,
                                  List<String> reviewedColumns,
                                  Set<String> errorColumns,
                                  String annotator,
                                  long createdAt,
                                  String labelIdPrefix) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "标签数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "标签快照标识");
        String row = ValueUtils.requireNotBlank(rowId, "标签行标识");
        if (rowLabel != 0 && rowLabel != 1) {
            throw new IllegalArgumentException("整行标签只能为 0 或 1");
        }
        if (reviewedColumns == null || reviewedColumns.isEmpty()
                || errorColumns == null || createdAt <= 0L) {
            throw new IllegalArgumentException("标签展开参数不能为空");
        }
        List<String> reviewed = new ArrayList<String>(reviewedColumns);
        Set<String> uniqueReviewed = new LinkedHashSet<String>(reviewed);
        if (uniqueReviewed.size() != reviewed.size()
                || !uniqueReviewed.containsAll(errorColumns)) {
            throw new IllegalArgumentException("已检查字段或异常字段不合法");
        }
        if (rowLabel == 0 && !errorColumns.isEmpty()) {
            throw new IllegalArgumentException("正常行不能包含异常字段");
        }
        if (rowLabel == 1 && errorColumns.isEmpty()) {
            throw new IllegalArgumentException("异常行必须包含异常字段");
        }
        String prefix = labelIdPrefix == null ? "annotation" : labelIdPrefix;
        List<CellLabel> labels = new ArrayList<CellLabel>(reviewed.size());
        for (String column : reviewed) {
            String name = ValueUtils.requireNotBlank(column, "已检查字段名");
            int value = errorColumns.contains(name) ? 1 : 0;
            String cellId = new CellCoordinate(dataset, snapshot, row, name)
                    .toCellId();
            String labelId = HashUtils.sha256Hex(prefix + "|" + cellId + "|"
                    + value);
            labels.add(new CellLabel(labelId, cellId, value, LabelSource.HUMAN,
                    1.0d, null, null, null, null, 1.0d, 0, null,
                    annotator, createdAt));
        }
        return Collections.unmodifiableList(labels);
    }

    /** 使用固定前缀生成标签，便于单独测试和离线调用。 */
    public List<CellLabel> expand(String datasetId,
                                  String snapshotId,
                                  String rowId,
                                  int rowLabel,
                                  List<String> reviewedColumns,
                                  Set<String> errorColumns,
                                  String annotator,
                                  long createdAt) {
        return expand(datasetId, snapshotId, rowId, rowLabel, reviewedColumns,
                errorColumns, annotator, createdAt, "annotation");
    }
}
