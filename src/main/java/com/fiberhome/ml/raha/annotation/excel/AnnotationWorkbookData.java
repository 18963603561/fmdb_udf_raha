package com.fiberhome.ml.raha.annotation.excel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存工作簿系统信息、有序业务字段、可检测字段和用户标注行。
 */
public final class AnnotationWorkbookData {

    /** 系统信息键值。 */
    private final Map<String, String> systemInfo;
    /** 模板业务字段顺序。 */
    private final List<String> businessColumns;
    /** 模板可检测字段顺序。 */
    private final List<String> detectableColumns;
    /** 用户标注行。 */
    private final List<AnnotationWorkbookRow> rows;

    public AnnotationWorkbookData(Map<String, String> systemInfo,
                                  List<String> businessColumns,
                                  List<String> detectableColumns,
                                  List<AnnotationWorkbookRow> rows) {
        if (systemInfo == null || businessColumns == null
                || detectableColumns == null || rows == null) {
            throw new IllegalArgumentException("标注工作簿解析结果不能为空");
        }
        this.systemInfo = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(systemInfo));
        this.businessColumns = Collections.unmodifiableList(
                new ArrayList<String>(businessColumns));
        this.detectableColumns = Collections.unmodifiableList(
                new ArrayList<String>(detectableColumns));
        this.rows = Collections.unmodifiableList(
                new ArrayList<AnnotationWorkbookRow>(rows));
    }

    public Map<String, String> getSystemInfo() { return systemInfo; }
    public List<String> getBusinessColumns() { return businessColumns; }
    public List<String> getDetectableColumns() { return detectableColumns; }
    public List<AnnotationWorkbookRow> getRows() { return rows; }
}
