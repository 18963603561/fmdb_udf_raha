package com.fiberhome.ml.raha.api;

import com.fiberhome.ml.raha.support.JsonUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 已提交模型集合摘要。
 */
public final class TrainResult {

    /** 全量或增量训练模式。 */
    private final String trainingMode;
    /** 本次实际训练字段。 */
    private final List<String> targetColumns;
    /** 新模型集合版本。 */
    private final String modelSetVersion;
    /** 成功列模型数量。 */
    private final int modelCount;
    /** 合并后的训练样本数。 */
    private final long trainingExampleCount;
    /** 同步调用耗时。 */
    private final long elapsedMillis;

    public TrainResult(String trainingMode, List<String> targetColumns,
                       String modelSetVersion, int modelCount,
                       long trainingExampleCount, long elapsedMillis) {
        this.trainingMode = trainingMode;
        this.targetColumns = targetColumns;
        this.modelSetVersion = modelSetVersion;
        this.modelCount = modelCount;
        this.trainingExampleCount = trainingExampleCount;
        this.elapsedMillis = elapsedMillis;
    }

    public String getTrainingMode() { return trainingMode; }
    public List<String> getTargetColumns() { return targetColumns; }
    public String getModelSetVersion() { return modelSetVersion; }
    public int getModelCount() { return modelCount; }
    public long getTrainingExampleCount() { return trainingExampleCount; }
    public long getElapsedMillis() { return elapsedMillis; }

    public String toJson() {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("trainingMode", trainingMode);
        values.put("targetColumns", targetColumns);
        values.put("modelSetVersion", modelSetVersion);
        values.put("modelCount", modelCount);
        values.put("trainingExampleCount", trainingExampleCount);
        values.put("elapsedMillis", elapsedMillis);
        return JsonUtils.toJson(values);
    }
}
