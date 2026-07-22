package com.fiberhome.ml.raha.service.task.batch;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存列批协调器返回给父任务阶段的摘要和阶段属性。
 */
public final class ColumnBatchExecutionOutcome {

    /** 父任务列批摘要。 */
    private final ColumnBatchExecutionSummary summary;
    /** 需要写回父任务运行结果的轻量属性。 */
    private final Map<String, Object> attributes;

    public ColumnBatchExecutionOutcome(
            ColumnBatchExecutionSummary summary,
            Map<String, Object> attributes) {
        if (summary == null || attributes == null) {
            throw new IllegalArgumentException("列批执行结果不能为空");
        }
        this.summary = summary;
        this.attributes = Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(attributes));
    }

    public ColumnBatchExecutionSummary getSummary() { return summary; }
    public Map<String, Object> getAttributes() { return attributes; }
}
