package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.fmdb.FmdbFeatureDictionaryCodec;
import com.fiberhome.ml.raha.fmdb.FmdbJsonCodec;
import com.fiberhome.ml.raha.fmdb.FmdbPartitionUtils;
import com.fiberhome.ml.raha.fmdb.FmdbTrainingArtifactRepository;
import com.fiberhome.ml.raha.fmdb.FmdbTrainingCellRecord;
import com.fiberhome.ml.raha.fmdb.FmdbTrainingColumnArtifactRecord;
import com.fiberhome.ml.raha.fmdb.FmdbTrainingExampleRecord;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationResult;
import com.fiberhome.ml.raha.label.propagation.ClusterPropagationSummary;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataset;
import com.fiberhome.ml.raha.model.training.ColumnTrainingExample;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.loader.RowIdentityColumns;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将合并训练快照的特征、聚类和标签冻结为最终三类训练物理产物。
 */
public final class TrainingArtifactMaterializationService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            TrainingArtifactMaterializationService.class);
    /** 训练产物仓储。 */
    private final FmdbTrainingArtifactRepository repository;
    /** 提供可测试物化时间的时钟。 */
    private final Clock clock;

    public TrainingArtifactMaterializationService(
            FmdbTrainingArtifactRepository repository, Clock clock) {
        if (repository == null || clock == null) {
            throw new IllegalArgumentException("训练物化服务依赖不能为空");
        }
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 物化并追加训练列级产物、训练单元格和实际训练样本。
     *
     * @param merge c1/o1 合并结果
     * @param features 训练快照特征和字典
     * @param clustering 聚类结果，可为空
     * @param propagation 标签传播结果
     * @param modelSetVersion 训练前冻结的模型集合版本
     * @param strategyPlanVersion 策略计划版本，可为空
     * @param profileJsonByColumn 画像 JSON，可为空
     * @param strategyPlanJsonByColumn 策略计划 JSON，可为空
     * @return 物化和写入数量
     */
    public TrainingArtifactMaterializationResult materialize(
            TrainingMergeResult merge,
            FeatureAssemblyResult features,
            ClusteringBatchResult clustering,
            LabelPropagationResult propagation,
            String modelSetVersion,
            String strategyPlanVersion,
            Map<String, String> profileJsonByColumn,
            Map<String, String> strategyPlanJsonByColumn) {
        return materialize(merge, features, clustering, propagation, modelSetVersion,
                strategyPlanVersion, null, profileJsonByColumn,
                strategyPlanJsonByColumn);
    }

    /**
     * 物化并冻结已经经过最终训练数据构建器筛选的实际入模样本。
     */
    public TrainingArtifactMaterializationResult materialize(
            TrainingMergeResult merge,
            FeatureAssemblyResult features,
            ClusteringBatchResult clustering,
            LabelPropagationResult propagation,
            String modelSetVersion,
            String strategyPlanVersion,
            Map<String, ColumnTrainingDataset> frozenTrainingDatasets,
            Map<String, String> profileJsonByColumn,
            Map<String, String> strategyPlanJsonByColumn) {
        if (merge == null || features == null || propagation == null) {
            throw new IllegalArgumentException("训练物化输入不能为空");
        }
        String modelSet = ValueUtils.requireNotBlank(modelSetVersion,
                "训练物化模型集合版本");
        long createdAt = clock.millis();
        if (createdAt <= 0L) {
            throw new IllegalStateException("训练物化时钟必须返回正时间");
        }
        Map<String, Map<String, Object>> rowValues = trustedRows(merge);
        Map<String, CellLabel> directLabels = new HashMap<String, CellLabel>();
        Map<String, CellLabel> propagatedLabels = new HashMap<String, CellLabel>();
        for (CellLabel label : propagation.getLabels()) {
            if (label.getLabelSource() == com.fiberhome.ml.raha.data.type.LabelSource.PROPAGATED) {
                propagatedLabels.put(label.getCellId(), label);
            } else {
                directLabels.put(label.getCellId(), label);
            }
        }
        Map<String, ClusterAssignment> assignments = assignments(clustering);
        Map<String, Map<String, ColumnTrainingExample>> frozenByColumn =
                indexFrozenExamples(frozenTrainingDatasets);
        String annotationBatchId = textValue(merge.getTrainingContext().get(
                "annotationBatchId"));
        List<FmdbTrainingCellRecord> cells = new ArrayList<FmdbTrainingCellRecord>();
        List<FmdbTrainingExampleRecord> examples =
                new ArrayList<FmdbTrainingExampleRecord>();
        long partitionTime = createdAt;
        String partitionMonth = FmdbPartitionUtils.month(partitionTime);
        for (SparseFeatureRow row : features.getRows()) {
            CellCoordinate coordinate = row.getCoordinate();
            if (coordinate == null || !merge.getTrainingSnapshotId().equals(
                    coordinate.getSnapshotId())) {
                throw new IllegalStateException("训练特征缺少可信训练快照坐标");
            }
            Map<String, Object> sourceRow = rowValues.get(coordinate.getRowId());
            if (sourceRow == null || !sourceRow.containsKey(row.getColumnName())) {
                throw new IllegalStateException("训练特征无法回填可信原始单元格值");
            }
            CellLabel direct = directLabels.get(row.getCellId());
            CellLabel propagated = propagatedLabels.get(row.getCellId());
            ClusterAssignment cluster = assignments.get(row.getCellId());
            String featureJson = FmdbJsonCodec.write(row.getValues());
            String summaryJson = FmdbJsonCodec.write(row.getSummary());
            String source = direct != null ? direct.getLabelSource().name()
                    : propagated == null ? null : propagated.getLabelSource().name();
            Double weight = direct != null ? direct.getSampleWeight()
                    : propagated == null ? null : propagated.getSampleWeight();
            FmdbTrainingCellRecord cell = new FmdbTrainingCellRecord(
                    merge.getTrainingBatchId(), merge.getDataset().getDatasetId(),
                    merge.getTrainingSnapshotId(), coordinate.getRowId(),
                    row.getColumnName(), row.getCellId(),
                    textValue(sourceRow.get(row.getColumnName())),
                    row.getFeatureDictionaryVersion(), featureJson, summaryJson,
                    cluster == null ? null : cluster.getClusterId(),
                    cluster == null ? null : cluster.getDistance(),
                    direct == null ? null : direct.getLabel(),
                    propagated == null ? null : propagated.getLabel(), source,
                    direct == null && propagated == null ? null : annotationBatchId,
                    weight, createdAt);
            cells.add(cell);
            CellLabel selected = direct == null ? propagated : direct;
            ColumnTrainingExample frozen = frozenByColumn.get(row.getColumnName())
                    == null ? null : frozenByColumn.get(row.getColumnName()).get(row.getCellId());
            if (frozenTrainingDatasets != null
                    && frozenTrainingDatasets.containsKey(row.getColumnName())
                    && frozen == null) {
                // 特征行没有进入最终训练集是允许的，但不应被误写入训练样本表。
                continue;
            }
            if (frozenTrainingDatasets == null && selected != null) {
                examples.add(new FmdbTrainingExampleRecord(modelSet,
                        merge.getTrainingBatchId(), merge.getDataset().getDatasetId(),
                        coordinate.getRowId(), row.getColumnName(), row.getCellId(),
                        textValue(sourceRow.get(row.getColumnName())),
                        row.getFeatureDictionaryVersion(), featureJson,
                        selected.getLabel(), selected.getLabelSource().name(),
                        annotationBatchId, selected.getSampleWeight(),
                        cluster == null ? null : cluster.getClusterId(), createdAt,
                        partitionMonth));
            } else if (frozen != null) {
                examples.add(new FmdbTrainingExampleRecord(modelSet,
                        merge.getTrainingBatchId(), merge.getDataset().getDatasetId(),
                        coordinate.getRowId(), row.getColumnName(), row.getCellId(),
                        textValue(sourceRow.get(row.getColumnName())),
                        row.getFeatureDictionaryVersion(),
                        FmdbJsonCodec.write(frozen.getFeatures()), frozen.getLabel(),
                        frozen.getLabelSource().name(), annotationBatchId,
                        frozen.getSampleWeight(),
                        cluster == null ? null : cluster.getClusterId(), createdAt,
                        partitionMonth));
            }
        }
        List<FmdbTrainingColumnArtifactRecord> artifacts = columnArtifacts(
                merge, features, clustering, propagation, modelSet, strategyPlanVersion,
                profileJsonByColumn, strategyPlanJsonByColumn, createdAt);
        long artifactWritten = repository.saveColumnArtifacts(artifacts);
        long cellWritten = repository.saveTrainingCells(cells);
        long exampleWritten = repository.saveTrainingExamples(examples);
        assertPersisted("训练列级产物", artifacts.size(),
                repository.countColumnArtifacts(merge.getDataset().getDatasetId(),
                        merge.getTrainingBatchId()),
                repository.isColumnArtifactPersistenceEnabled());
        assertPersisted("训练单元格", cells.size(),
                repository.countTrainingCells(merge.getDataset().getDatasetId(),
                        merge.getTrainingBatchId()),
                repository.isTrainingCellPersistenceEnabled());
        assertPersisted("训练样本", examples.size(),
                repository.countTrainingExamples(merge.getDataset().getDatasetId(),
                        partitionMonth, modelSet),
                repository.isTrainingExamplePersistenceEnabled());
        LOGGER.info("训练产物物化完成，trainingBatchId={}，columnCount={}，"
                        + "cellCount={}，exampleCount={}", merge.getTrainingBatchId(),
                artifacts.size(), cells.size(), examples.size());
        return new TrainingArtifactMaterializationResult(artifactWritten, cellWritten,
                exampleWritten, cells.size(), examples.size(), partitionMonth, modelSet);
    }

    /** 判断最终训练样本是否已开启物理持久化。 */
    public boolean isTrainingExamplePersistenceEnabled() {
        return repository.isTrainingExamplePersistenceEnabled();
    }

    /** 从最终训练样本表按模型集合恢复一个字段的训练数据。 */
    public ColumnTrainingDataset loadFrozenTrainingDataset(
            String datasetId,
            String partitionMonth,
            String modelSetVersion,
            String columnName,
            String featureDictionaryVersion,
            int featureDimension) {
        return repository.loadFrozenTrainingDataset(datasetId, partitionMonth,
                modelSetVersion, columnName, featureDictionaryVersion, featureDimension);
    }

    private static void assertPersisted(String name, int expected, long actual,
                                        boolean enabled) {
        if (enabled && expected != actual) {
            throw new IllegalStateException(name + "物理记录数量不一致，expected="
                    + expected + "，actual=" + actual);
        }
    }

    private static Map<String, Map<String, ColumnTrainingExample>> indexFrozenExamples(
            Map<String, ColumnTrainingDataset> datasets) {
        Map<String, Map<String, ColumnTrainingExample>> result =
                new LinkedHashMap<String, Map<String, ColumnTrainingExample>>();
        if (datasets == null) {
            return result;
        }
        for (Map.Entry<String, ColumnTrainingDataset> entry : datasets.entrySet()) {
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("冻结训练数据不能包含空字段");
            }
            Map<String, ColumnTrainingExample> byCell =
                    new LinkedHashMap<String, ColumnTrainingExample>();
            for (ColumnTrainingExample example : entry.getValue().getExamples()) {
                if (byCell.put(example.getCellId(), example) != null) {
                    throw new IllegalArgumentException("冻结训练数据包含重复单元格");
                }
            }
            result.put(entry.getKey(), byCell);
        }
        return result;
    }

    private static List<FmdbTrainingColumnArtifactRecord> columnArtifacts(
            TrainingMergeResult merge,
            FeatureAssemblyResult features,
            ClusteringBatchResult clustering,
            LabelPropagationResult propagation,
            String modelSetVersion,
            String strategyPlanVersion,
            Map<String, String> profiles,
            Map<String, String> strategies,
            long createdAt) {
        List<FmdbTrainingColumnArtifactRecord> result =
                new ArrayList<FmdbTrainingColumnArtifactRecord>();
        for (Map.Entry<String, FeatureDictionary> entry
                : features.getDictionaries().entrySet()) {
            String column = entry.getKey();
            String profile = profiles == null ? null : profiles.get(column);
            String strategy = strategies == null ? null : strategies.get(column);
            ColumnClusteringResult clusteringResult = clustering == null ? null
                    : clustering.getResults().get(column);
            List<ClusterPropagationSummary> propagationSummaries =
                    new ArrayList<ClusterPropagationSummary>();
            if (propagation != null) {
                for (ClusterPropagationSummary summary : propagation.getSummaries()) {
                    if (column.equals(summary.getColumnName())) {
                        propagationSummaries.add(summary);
                    }
                }
            }
            result.add(new FmdbTrainingColumnArtifactRecord(
                    merge.getTrainingBatchId(), merge.getDataset().getDatasetId(),
                    nullableText(merge.getTrainingContext().get("sourceVersion")),
                    merge.getDataset().getSchemaHash(), merge.getMergeAlgorithmVersion(),
                    FmdbJsonCodec.write(merge.getTrainingContext()), column,
                    profile == null ? null : "profile-v1", profile, strategyPlanVersion,
                    strategy, entry.getValue().getVersion(),
                    FmdbFeatureDictionaryCodec.write(entry.getValue()),
                    clusteringResult == null ? null : clusteringResult.getClusterVersion(),
                    clusteringResult == null ? null : FmdbJsonCodec.write(
                            clusterSummary(clusteringResult)),
                    propagationSummaries.isEmpty() ? null
                            : FmdbJsonCodec.write(propagationSummaries), createdAt));
        }
        return result;
    }

    private static Map<String, Object> clusterSummary(
            ColumnClusteringResult clustering) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("algorithm", clustering.getAlgorithm());
        result.put("distanceMetric", clustering.getDistanceMetric().name());
        result.put("requestedClusterCount", clustering.getRequestedClusterCount());
        result.put("effectiveClusterCount", clustering.getEffectiveClusterCount());
        result.put("assignmentCount", clustering.getAssignments().size());
        result.put("status", clustering.getStatus().name());
        result.put("message", clustering.getMessage());
        result.put("createdAt", clustering.getCreatedAt());
        return result;
    }

    private static Map<String, ClusterAssignment> assignments(
            ClusteringBatchResult clustering) {
        Map<String, ClusterAssignment> result =
                new LinkedHashMap<String, ClusterAssignment>();
        if (clustering == null) {
            return result;
        }
        for (ColumnClusteringResult column : clustering.getResults().values()) {
            for (ClusterAssignment assignment : column.getAssignments()) {
                result.put(assignment.getCellId(), assignment);
            }
        }
        return result;
    }

    private static String textValue(Object value) {
        return value == null ? null : String.valueOf((Object) value);
    }

    private static String nullableText(Object value) {
        if (value == null || "null".equals(String.valueOf((Object) value))) {
            return null;
        }
        return String.valueOf((Object) value);
    }

    private static Map<String, Map<String, Object>> trustedRows(
            TrainingMergeResult merge) {
        Map<String, Map<String, Object>> result =
                new LinkedHashMap<String, Map<String, Object>>();
        for (Row row : merge.getDataset().getDataFrame().collectAsList()) {
            String rowId = String.valueOf((Object) row.getAs(
                    merge.getDataset().getRowIdColumn()));
            Map<String, Object> values = new LinkedHashMap<String, Object>();
            StructType schema = merge.getDataset().getDataFrame().schema();
            for (StructField field : schema.fields()) {
                if (!RowIdentityColumns.isTechnical(field.name())) {
                    values.put(field.name(), row.getAs(field.name()));
                }
            }
            if (result.put(rowId, values) != null) {
                throw new IllegalStateException("训练快照包含重复逻辑行");
            }
        }
        return result;
    }
}
