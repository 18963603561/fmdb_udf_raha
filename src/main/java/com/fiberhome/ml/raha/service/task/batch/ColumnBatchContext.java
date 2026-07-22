package com.fiberhome.ml.raha.service.task.batch;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存子任务所属父任务和当前列批范围。
 */
public final class ColumnBatchContext {

    /** 父任务标识。 */
    private final String parentJobId;
    /** 当前列批标识。 */
    private final String batchId;
    /** 当前列批索引。 */
    private final int batchIndex;
    /** 父任务列批总数。 */
    private final int batchCount;
    /** 当前列批字段。 */
    private final List<String> columns;

    public ColumnBatchContext(String parentJobId,
                              ColumnBatch batch,
                              int batchCount) {
        if (batch == null || batchCount <= 0
                || batch.getBatchIndex() >= batchCount) {
            throw new IllegalArgumentException("列批上下文参数非法");
        }
        this.parentJobId = ValueUtils.requireNotBlank(parentJobId, "父任务标识");
        this.batchId = batch.getBatchId();
        this.batchIndex = batch.getBatchIndex();
        this.batchCount = batchCount;
        this.columns = Collections.unmodifiableList(
                new ArrayList<String>(batch.getColumns()));
    }

    public String getParentJobId() { return parentJobId; }
    public String getBatchId() { return batchId; }
    public int getBatchIndex() { return batchIndex; }
    public int getBatchCount() { return batchCount; }
    public List<String> getColumns() { return columns; }
}
