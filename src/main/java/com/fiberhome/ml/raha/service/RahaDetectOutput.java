package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.data.DetectionResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存批量模型检测结果、字段模型版本和字段失败原因。
 */
public final class RahaDetectOutput {

    /** 成功生成的单元格检测结果。 */
    private final List<DetectionResult> results;
    /** 成功字段到模型版本的映射。 */
    private final Map<String, String> modelVersions;
    /** 失败字段到安全错误摘要的映射。 */
    private final Map<String, String> failedColumns;

    public RahaDetectOutput(List<DetectionResult> results,
                            Map<String, String> modelVersions,
                            Map<String, String> failedColumns) {
        if (results == null || modelVersions == null || failedColumns == null) {
            throw new IllegalArgumentException("检测服务输出不能为空");
        }
        this.results = Collections.unmodifiableList(
                new ArrayList<DetectionResult>(results));
        this.modelVersions = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(modelVersions));
        this.failedColumns = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(failedColumns));
    }

    public List<DetectionResult> getResults() { return results; }
    public Map<String, String> getModelVersions() { return modelVersions; }
    public Map<String, String> getFailedColumns() { return failedColumns; }
}
