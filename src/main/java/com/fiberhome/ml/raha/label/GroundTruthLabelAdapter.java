package com.fiberhome.ml.raha.label;

import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.JobType;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.sampling.AnnotationTask;
import com.fiberhome.ml.raha.sampling.AnnotationTaskStatus;
import com.fiberhome.ml.raha.strategy.SparkStrategySupport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 在评测模式下比较脏表和真值表同一位置，只生成错误零一标签。
 */
public final class GroundTruthLabelAdapter {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(GroundTruthLabelAdapter.class);
    /** 提供可测试标签时间的时钟。 */
    private final Clock clock;

    public GroundTruthLabelAdapter(Clock clock) {
        if (clock == null) {
            throw new IllegalArgumentException("真值标注适配器时钟不能为空");
        }
        this.clock = clock;
    }

    /**
     * 比较待标注行在脏表和真值表中的可检测字段，只生成零一错误标签。
     *
     * @param jobType 当前任务模式，必须为评测模式
     * @param task 待标注元组任务
     * @param dirtyDataset 脏数据集
     * @param groundTruthDataset 真值数据集
     * @return 已完成任务和直接真值标签
     */
    public GroundTruthLabelingResult label(JobType jobType,
                                           AnnotationTask task,
                                           RahaDataset dirtyDataset,
                                           RahaDataset groundTruthDataset) {
        if (jobType != JobType.EVALUATION) {
            throw new IllegalStateException("真值自动标注只允许在评测模式使用");
        }
        if (task == null || dirtyDataset == null || groundTruthDataset == null
                || dirtyDataset.getDataFrame() == null
                || groundTruthDataset.getDataFrame() == null) {
            throw new IllegalArgumentException("真值自动标注参数不能为空");
        }
        if (task.getStatus() != AnnotationTaskStatus.PENDING) {
            throw new IllegalStateException("只有待标注任务可以使用真值自动标注");
        }
        if (!dirtyDataset.getRowIdColumn().equals(groundTruthDataset.getRowIdColumn())) {
            throw new IllegalArgumentException("脏表和真值表行标识字段不一致");
        }
        Set<String> truthColumns = new HashSet<String>();
        for (ColumnMetadata column : groundTruthDataset.getColumns()) {
            truthColumns.add(column.getName());
        }
        LOGGER.info("开始读取评测真值，taskId={}，datasetId={}",
                task.getTaskId(), dirtyDataset.getDatasetId());
        Row dirtyRow = findSingleRow(dirtyDataset, task.getRowId());
        Row truthRow = findSingleRow(groundTruthDataset, task.getRowId());
        long createdAt = clock.millis();
        List<CellLabel> labels = new ArrayList<CellLabel>();
        for (ColumnMetadata column : dirtyDataset.getColumns()) {
            if (!column.isDetectable()) {
                continue;
            }
            if (!truthColumns.contains(column.getName())) {
                throw new IllegalArgumentException("真值表缺少待标注字段：" + column.getName());
            }
            Object dirtyValue = dirtyRow.getAs(column.getName());
            Object truthValue = truthRow.getAs(column.getName());
            int label = Objects.equals(dirtyValue, truthValue) ? 0 : 1;
            CellCoordinate coordinate = new CellCoordinate(dirtyDataset.getDatasetId(),
                    dirtyDataset.getSnapshotId(), task.getRowId(), column.getName());
            labels.add(new CellLabel(coordinate.toCellId(), label,
                    LabelSource.GROUND_TRUTH, 1.0d, null, null,
                    "ground-truth-adapter", createdAt));
        }
        AnnotationTask completedTask = task.snapshot();
        completedTask.complete(createdAt);
        LOGGER.info("评测真值标注完成，taskId={}，labelCount={}",
                task.getTaskId(), labels.size());
        return new GroundTruthLabelingResult(completedTask, labels);
    }

    private static Row findSingleRow(RahaDataset dataset, String rowId) {
        Dataset<Row> matches = dataset.getDataFrame().filter(
                SparkStrategySupport.quotedColumn(dataset.getRowIdColumn())
                        .cast("string").equalTo(rowId)).limit(2);
        List<Row> rows = matches.collectAsList();
        if (rows.size() != 1) {
            throw new IllegalArgumentException("数据集必须包含且仅包含一个目标行，datasetId="
                    + dataset.getDatasetId());
        }
        return rows.get(0);
    }
}
