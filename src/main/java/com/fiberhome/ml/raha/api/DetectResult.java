package com.fiberhome.ml.raha.api;

import com.fiberhome.ml.raha.support.JsonUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 已提交检测批次摘要。
 */
public final class DetectResult {

    /** 检测批次标识。 */
    private final String detectionBatchId;
    /** 实际检测目标字段。 */
    private final List<String> targetColumns;
    /** 评估单元格数量。 */
    private final long evaluatedCellCount;
    /** 疑似错误单元格数量。 */
    private final long detectedCellCount;
    /** 标准结果位置。 */
    private final String resultLocation;
    /** 同步调用耗时。 */
    private final long elapsedMillis;

    public DetectResult(String detectionBatchId, List<String> targetColumns,
                        long evaluatedCellCount, long detectedCellCount,
                        String resultLocation, long elapsedMillis) {
        this.detectionBatchId = detectionBatchId;
        this.targetColumns = targetColumns;
        this.evaluatedCellCount = evaluatedCellCount;
        this.detectedCellCount = detectedCellCount;
        this.resultLocation = resultLocation;
        this.elapsedMillis = elapsedMillis;
    }

    public String getDetectionBatchId() { return detectionBatchId; }
    public List<String> getTargetColumns() { return targetColumns; }
    public long getEvaluatedCellCount() { return evaluatedCellCount; }
    public long getDetectedCellCount() { return detectedCellCount; }
    public String getResultLocation() { return resultLocation; }
    public long getElapsedMillis() { return elapsedMillis; }

    public String toJson() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("detectionBatchId", detectionBatchId);
        values.put("targetColumns", targetColumns);
        values.put("evaluatedCellCount", evaluatedCellCount);
        values.put("detectedCellCount", detectedCellCount);
        values.put("resultLocation", resultLocation);
        values.put("elapsedMillis", elapsedMillis);
        return JsonUtils.toJson(values);
    }
}
