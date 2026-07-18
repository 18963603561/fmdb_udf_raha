package com.fiberhome.ml.raha.api;

import com.fiberhome.ml.raha.support.JsonUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 已提交采样批次摘要。
 */
public final class SampleResult {

    /** 采样批次标识。 */
    private final String sampleBatchId;
    /** 展开后的目标字段。 */
    private final List<String> targetColumns;
    /** 实际采样元组数。 */
    private final long selectedTupleCount;
    /** 标准结果位置。 */
    private final String resultLocation;
    /** 同步调用耗时。 */
    private final long elapsedMillis;

    public SampleResult(String sampleBatchId, List<String> targetColumns,
                        long selectedTupleCount, String resultLocation,
                        long elapsedMillis) {
        this.sampleBatchId = sampleBatchId;
        this.targetColumns = targetColumns;
        this.selectedTupleCount = selectedTupleCount;
        this.resultLocation = resultLocation;
        this.elapsedMillis = elapsedMillis;
    }

    public String getSampleBatchId() { return sampleBatchId; }
    public List<String> getTargetColumns() { return targetColumns; }
    public long getSelectedTupleCount() { return selectedTupleCount; }
    public String getResultLocation() { return resultLocation; }
    public long getElapsedMillis() { return elapsedMillis; }

    public String toJson() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("sampleBatchId", sampleBatchId);
        values.put("targetColumns", targetColumns);
        values.put("selectedTupleCount", selectedTupleCount);
        values.put("resultLocation", resultLocation);
        values.put("elapsedMillis", elapsedMillis);
        return JsonUtils.toJson(values);
    }
}
