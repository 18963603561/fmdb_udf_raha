package com.fiberhome.ml.raha.service.task.batch;

import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存列批父任务的子任务轨迹和字段级汇总。
 */
public final class ColumnBatchExecutionSummary {

    /** 父任务标识。 */
    private final String parentJobId;
    /** 父任务类型。 */
    private final JobType jobType;
    /** 列批参数。 */
    private final ColumnBatchOptions options;
    /** 目标字段总数。 */
    private final int targetColumnCount;
    /** 全局模型集合版本或检测批次标识。 */
    private final String globalBatchVersion;
    /** 子任务结果。 */
    private final List<ColumnBatchTaskResult> taskResults;
    /** 成功处理字段。 */
    private final List<String> succeededColumns;
    /** 失败字段和原因。 */
    private final Map<String, String> failedColumns;
    /** 父任务总耗时。 */
    private final long elapsedMillis;

    public ColumnBatchExecutionSummary(
            String parentJobId,
            JobType jobType,
            ColumnBatchOptions options,
            int targetColumnCount,
            String globalBatchVersion,
            List<ColumnBatchTaskResult> taskResults,
            List<String> succeededColumns,
            Map<String, String> failedColumns,
            long elapsedMillis) {
        if (jobType == null || options == null || targetColumnCount <= 0
                || taskResults == null || succeededColumns == null
                || failedColumns == null || elapsedMillis < 0L) {
            throw new IllegalArgumentException("列批父任务摘要参数非法");
        }
        this.parentJobId = ValueUtils.requireNotBlank(parentJobId, "父任务标识");
        this.jobType = jobType;
        this.options = options;
        this.targetColumnCount = targetColumnCount;
        this.globalBatchVersion = ValueUtils.requireNotBlank(
                globalBatchVersion, "列批全局版本");
        this.taskResults = Collections.unmodifiableList(
                new ArrayList<ColumnBatchTaskResult>(taskResults));
        this.succeededColumns = Collections.unmodifiableList(
                new ArrayList<String>(succeededColumns));
        this.failedColumns = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(failedColumns));
        this.elapsedMillis = elapsedMillis;
    }

    public int getSucceededBatchCount() {
        int count = 0;
        for (ColumnBatchTaskResult result : taskResults) {
            if (result.getStatus() != com.fiberhome.ml.raha.data.type.JobStatus.FAILED) {
                count++;
            }
        }
        return count;
    }

    public int getBatchCount() { return taskResults.size(); }
    public Map<String, String> getFailedColumns() { return failedColumns; }

    /**
     * 转换为可直接写入任务结果摘要的结构。
     *
     * @return 父任务有序摘要
     */
    public Map<String, Object> toSummaryMap() {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("parentJobId", parentJobId);
        result.put("jobType", jobType.name());
        result.put("columnBatchSize", Integer.valueOf(options.getColumnBatchSize()));
        result.put("columnBatchCount", Integer.valueOf(taskResults.size()));
        result.put("maxParallelColumnBatches",
                Integer.valueOf(options.getMaxParallelColumnBatches()));
        result.put("batchRvdEnabled",
                Boolean.valueOf(options.isBatchRvdEnabled()));
        result.put("batchRvdScope", "IN_BATCH");
        result.put("targetColumnCount", Integer.valueOf(targetColumnCount));
        if (jobType == JobType.TRAINING) {
            result.put("modelSetVersion", globalBatchVersion);
        } else {
            result.put("detectionBatchId", globalBatchVersion);
            result.put("currentBatchId", globalBatchVersion);
        }
        List<Map<String, Object>> children =
                new ArrayList<Map<String, Object>>(taskResults.size());
        for (ColumnBatchTaskResult taskResult : taskResults) {
            children.add(taskResult.toSummaryMap());
        }
        result.put("columnBatchTasks", children);
        result.put("succeededColumns", succeededColumns);
        result.put("failedColumns", failedColumns);
        result.put("elapsedMillis", Long.valueOf(elapsedMillis));
        return result;
    }
}
