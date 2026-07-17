package com.fiberhome.ml.raha.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 同步训练请求，支持多个采样批次和可选父模型集合。
 */
public final class TrainRequest {

    /** 一个或多个已标注采样批次。 */
    private final List<String> sampleBatchIds;
    /** 可选训练目标字段。 */
    private final List<String> targetColumns;
    /** 可选父模型集合版本。 */
    private final String baseModelSetVersion;

    public TrainRequest(List<String> sampleBatchIds, List<String> targetColumns,
                        String baseModelSetVersion) {
        this.sampleBatchIds = immutable(sampleBatchIds);
        this.targetColumns = immutable(targetColumns);
        this.baseModelSetVersion = baseModelSetVersion;
    }

    private static List<String> immutable(List<String> values) {
        return values == null ? Collections.<String>emptyList()
                : Collections.unmodifiableList(new ArrayList<String>(values));
    }

    public List<String> getSampleBatchIds() { return sampleBatchIds; }
    public List<String> getTargetColumns() { return targetColumns; }
    public String getBaseModelSetVersion() { return baseModelSetVersion; }
}
