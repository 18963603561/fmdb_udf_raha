package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.CellLabelRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 使用 Spark 全外连接比较脏表和真值表，生成全量单元格真值标签。
 */
public final class GroundTruthDifferenceService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            GroundTruthDifferenceService.class);
    /** 真值标签仓储。 */
    private final CellLabelRepository repository;
    /** 提供可测试标签时间的时钟。 */
    private final Clock clock;

    public GroundTruthDifferenceService(CellLabelRepository repository, Clock clock) {
        if (repository == null || clock == null) {
            throw new IllegalArgumentException("真值差异服务依赖不能为空");
        }
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 比较两个数据集的行集合和可检测字段，并事务保存全量真值标签。
     *
     * @param jobId 评测任务标识
     * @param dirtyDataset 待检测脏数据集
     * @param groundTruthDataset 对应真值数据集
     * @param version 标签仓储业务版本
     * @return 单元格真值标签和正负类别数量
     */
    public GroundTruthDifferenceResult compareAndSave(String jobId,
                                                       RahaDataset dirtyDataset,
                                                       RahaDataset groundTruthDataset,
                                                       ArtifactVersion version) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "评测任务标识");
        validateDatasets(dirtyDataset, groundTruthDataset, version);
        LOGGER.info("开始读取并比较评测真值，jobId={}，datasetId={}，truthDatasetId={}",
                validatedJobId, dirtyDataset.getDatasetId(),
                groundTruthDataset.getDatasetId());
        try {
            List<ColumnMetadata> detectableColumns = detectableColumns(
                    dirtyDataset, groundTruthDataset);
            Dataset<Row> dirty = dirtyDataset.getDataFrame().alias("dirty");
            Dataset<Row> truth = groundTruthDataset.getDataFrame().alias("truth");
            Column dirtyId = dirty.col(dirtyDataset.getRowIdColumn()).cast("string");
            Column truthId = truth.col(groundTruthDataset.getRowIdColumn()).cast("string");
            List<Column> projections = new ArrayList<Column>();
            projections.add(functions.coalesce(dirtyId, truthId).alias("_row_id"));
            projections.add(dirtyId.alias("_dirty_row_id"));
            projections.add(truthId.alias("_truth_row_id"));
            for (int index = 0; index < detectableColumns.size(); index++) {
                String columnName = detectableColumns.get(index).getName();
                projections.add(dirty.col(columnName).alias("_dirty_" + index));
                projections.add(truth.col(columnName).alias("_truth_" + index));
            }
            Dataset<Row> joined = dirty.join(truth, dirtyId.equalTo(truthId), "full_outer")
                    .select(projections.toArray(new Column[projections.size()]));
            List<Row> rows = joined.collectAsList();
            Set<String> rowIds = new LinkedHashSet<String>();
            List<CellLabel> labels = new ArrayList<CellLabel>();
            long positives = 0L;
            long createdAt = clock.millis();
            for (Row row : rows) {
                String rowId = row.getAs("_row_id");
                String dirtyRowId = row.getAs("_dirty_row_id");
                String truthRowId = row.getAs("_truth_row_id");
                // 行集合不一致或重复会使单元格真值失去一一对应关系，必须拒绝评测。
                if (rowId == null || dirtyRowId == null || truthRowId == null
                        || !rowIds.add(rowId)) {
                    throw new IllegalArgumentException("脏表和真值表行集合不一致或行标识重复");
                }
                for (int index = 0; index < detectableColumns.size(); index++) {
                    Object dirtyValue = row.getAs("_dirty_" + index);
                    Object truthValue = row.getAs("_truth_" + index);
                    int labelValue = Objects.deepEquals(dirtyValue, truthValue) ? 0 : 1;
                    positives += labelValue;
                    CellCoordinate coordinate = new CellCoordinate(
                            dirtyDataset.getDatasetId(), dirtyDataset.getSnapshotId(),
                            rowId, detectableColumns.get(index).getName());
                    labels.add(new CellLabel(coordinate.toCellId(), labelValue,
                            LabelSource.GROUND_TRUTH, 1.0d, null, null,
                            "ground-truth-difference", createdAt));
                }
            }
            repository.saveLabels(validatedJobId, labels, version, createdAt);
            GroundTruthDifferenceResult result = new GroundTruthDifferenceResult(
                    labels, positives, labels.size() - positives);
            LOGGER.info("评测真值比较完成，jobId={}，labelCount={}，"
                            + "positiveCount={}，negativeCount={}",
                    validatedJobId, labels.size(), result.getPositiveCount(),
                    result.getNegativeCount());
            return result;
        } catch (RuntimeException exception) {
            // Spark 读取、连接或标签仓储失败时记录数据集上下文和异常堆栈。
            LOGGER.error("评测真值比较失败，jobId={}，datasetId={}，truthDatasetId={}",
                    validatedJobId, dirtyDataset.getDatasetId(),
                    groundTruthDataset.getDatasetId(), exception);
            throw exception;
        }
    }

    private static void validateDatasets(RahaDataset dirtyDataset,
                                         RahaDataset truthDataset,
                                         ArtifactVersion version) {
        if (dirtyDataset == null || truthDataset == null || version == null
                || dirtyDataset.getDataFrame() == null
                || truthDataset.getDataFrame() == null) {
            throw new IllegalArgumentException("脏表、真值表和仓储版本不能为空");
        }
        if (!dirtyDataset.getRowIdColumn().equals(truthDataset.getRowIdColumn())) {
            throw new IllegalArgumentException("脏表和真值表行标识字段不一致");
        }
    }

    private static List<ColumnMetadata> detectableColumns(RahaDataset dirtyDataset,
                                                           RahaDataset truthDataset) {
        Set<String> truthColumns = new HashSet<String>();
        for (ColumnMetadata column : truthDataset.getColumns()) {
            truthColumns.add(column.getName());
        }
        List<ColumnMetadata> detectable = new ArrayList<ColumnMetadata>();
        for (ColumnMetadata column : dirtyDataset.getColumns()) {
            if (column.isDetectable()) {
                if (!truthColumns.contains(column.getName())) {
                    throw new IllegalArgumentException("真值表缺少可检测字段：" + column.getName());
                }
                detectable.add(column);
            }
        }
        return detectable;
    }
}
