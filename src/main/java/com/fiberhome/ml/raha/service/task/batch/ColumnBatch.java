package com.fiberhome.ml.raha.service.task.batch;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 描述一个稳定的字段子批次。
 */
public final class ColumnBatch {

    /** 从零开始的批次索引。 */
    private final int batchIndex;
    /** 父任务内唯一的批次标识。 */
    private final String batchId;
    /** 当前批次按来源模式顺序保存的字段。 */
    private final List<String> columns;

    public ColumnBatch(int batchIndex, String batchId, List<String> columns) {
        if (batchIndex < 0 || columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("列批索引和字段不能为空");
        }
        this.batchIndex = batchIndex;
        this.batchId = ValueUtils.requireNotBlank(batchId, "列批标识");
        this.columns = Collections.unmodifiableList(
                new ArrayList<String>(columns));
    }

    public int getBatchIndex() {
        return batchIndex;
    }

    public String getBatchId() {
        return batchId;
    }

    public List<String> getColumns() {
        return columns;
    }
}
