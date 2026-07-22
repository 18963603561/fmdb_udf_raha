package com.fiberhome.ml.raha.service.task.batch;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 按来源模式顺序把有效业务字段拆成固定大小的列批。
 */
public final class ColumnBatchPlanner {

    /**
     * 生成稳定列批，重复字段只保留第一次出现的位置。
     *
     * @param parentJobId 父任务标识
     * @param columns 有效业务字段
     * @param batchSize 每批最大字段数
     * @return 不可变列批列表
     */
    public List<ColumnBatch> plan(String parentJobId,
                                  List<String> columns,
                                  int batchSize) {
        String parent = ValueUtils.requireNotBlank(parentJobId, "父任务标识");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("列批字段不能为空");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("列批大小必须大于零");
        }
        Set<String> unique = new LinkedHashSet<String>();
        for (String column : columns) {
            unique.add(ValueUtils.requireNotBlank(column, "列批字段"));
        }
        List<String> ordered = new ArrayList<String>(unique);
        List<ColumnBatch> batches = new ArrayList<ColumnBatch>();
        int batchIndex = 0;
        for (int start = 0; start < ordered.size(); start += batchSize) {
            int end = Math.min(start + batchSize, ordered.size());
            String batchId = parent + "-column-batch-"
                    + String.format("%03d", batchIndex + 1);
            batches.add(new ColumnBatch(batchIndex, batchId,
                    ordered.subList(start, end)));
            batchIndex++;
        }
        return Collections.unmodifiableList(batches);
    }
}
