package com.fiberhome.ml.raha.service.task.batch;

import com.fiberhome.ml.raha.data.type.JobStatus;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存单个列批子任务的状态、字段范围和耗时。
 */
public final class ColumnBatchTaskResult {

    /** 当前列批。 */
    private final ColumnBatch batch;
    /** 实际子任务标识。 */
    private final String childJobId;
    /** 子任务最终状态。 */
    private final JobStatus status;
    /** 子任务耗时。 */
    private final long elapsedMillis;
    /** 可安全返回的失败编码。 */
    private final String errorCode;
    /** 可安全返回的失败说明。 */
    private final String errorMessage;

    public ColumnBatchTaskResult(ColumnBatch batch,
                                 String childJobId,
                                 JobStatus status,
                                 long elapsedMillis,
                                 String errorCode,
                                 String errorMessage) {
        if (batch == null || status == null || elapsedMillis < 0L) {
            throw new IllegalArgumentException("列批子任务结果参数非法");
        }
        this.batch = batch;
        this.childJobId = childJobId;
        this.status = status;
        this.elapsedMillis = elapsedMillis;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public ColumnBatch getBatch() { return batch; }
    public String getChildJobId() { return childJobId; }
    public JobStatus getStatus() { return status; }

    /**
     * 转换为父任务结果摘要中的轻量结构。
     *
     * @return 子任务有序摘要
     */
    public Map<String, Object> toSummaryMap() {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("batchId", batch.getBatchId());
        result.put("batchIndex", Integer.valueOf(batch.getBatchIndex()));
        result.put("columns", Collections.unmodifiableList(
                new ArrayList<String>(batch.getColumns())));
        result.put("childJobId", childJobId);
        result.put("status", status.name());
        result.put("elapsedMillis", Long.valueOf(elapsedMillis));
        result.put("errorCode", errorCode);
        result.put("errorMessage", errorMessage);
        return result;
    }
}
