package com.fiberhome.ml.raha.train;

import com.fiberhome.ml.raha.api.TrainRequest;
import com.fiberhome.ml.raha.api.TrainResult;
import com.fiberhome.ml.raha.config.RahaConfig;
import com.fiberhome.ml.raha.data.CellLabel;
import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.TargetColumnResolver;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.FeatureVectorizer;
import com.fiberhome.ml.raha.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.label.LabelStore;
import com.fiberhome.ml.raha.model.ColumnModelTrainer;
import com.fiberhome.ml.raha.model.ModelStore;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.model.RahaModelSet;
import com.fiberhome.ml.raha.profile.ColumnProfiler;
import com.fiberhome.ml.raha.sample.SampleBatch;
import com.fiberhome.ml.raha.sample.SampleStore;
import com.fiberhome.ml.raha.sample.SampleTuple;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyPlanner;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;
import com.fiberhome.ml.raha.support.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 同步全量和增量训练用例。
 */
public final class RahaTrainService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaTrainService.class);
    /** 采样存储。 */
    private final SampleStore sampleStore;
    /** 标签存储。 */
    private final LabelStore labelStore;
    /** 模型存储。 */
    private final ModelStore modelStore;
    /** 数据加载器。 */
    private final FmdbDatasetLoader datasetLoader;
    /** 列画像。 */
    private final ColumnProfiler columnProfiler;
    /** 策略计划。 */
    private final StrategyPlanner strategyPlanner;
    /** 训练样本构建器。 */
    private final TrainingDatasetBuilder datasetBuilder;
    /** 增量样本合并器。 */
    private final IncrementalTrainingDatasetBuilder incrementalBuilder;
    /** 分类器训练器。 */
    private final ColumnModelTrainer modelTrainer;
    /** 特征向量构建器。 */
    private final FeatureVectorizer vectorizer;
    /** 根配置。 */
    private final RahaConfig config;

    public RahaTrainService(SampleStore sampleStore, LabelStore labelStore,
                            ModelStore modelStore, FmdbDatasetLoader datasetLoader,
                            ColumnProfiler columnProfiler, StrategyPlanner strategyPlanner,
                            TrainingDatasetBuilder datasetBuilder,
                            IncrementalTrainingDatasetBuilder incrementalBuilder,
                            ColumnModelTrainer modelTrainer, FeatureVectorizer vectorizer,
                            RahaConfig config) {
        this.sampleStore = sampleStore;
        this.labelStore = labelStore;
        this.modelStore = modelStore;
        this.datasetLoader = datasetLoader;
        this.columnProfiler = columnProfiler;
        this.strategyPlanner = strategyPlanner;
        this.datasetBuilder = datasetBuilder;
        this.incrementalBuilder = incrementalBuilder;
        this.modelTrainer = modelTrainer;
        this.vectorizer = vectorizer;
        this.config = config;
    }

    public TrainResult train(TrainRequest request) {
        long startedAt = System.currentTimeMillis();
        List<String> batchIds = unique(request.getSampleBatchIds(), "采样批次");
        if (batchIds.isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST, "训练至少需要一个采样批次");
        }
        List<SampleBatch> batches = sampleStore.loadBatches(batchIds);
        if (batches.size() != batchIds.size()) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST, "存在未提交的采样批次");
        }
        sortBatches(batches, batchIds);
        SampleBatch first = validateBatches(batches);
        List<CellLabel> labels = labelStore.load(batchIds);
        validateLabels(labels, first);
        List<String> availableTargets = targetUnion(batches);
        List<String> labeledTargets = labeledTargets(labels, availableTargets);
        List<String> targets = request.getTargetColumns().isEmpty()
                ? labeledTargets : TargetColumnResolver.resolve(request.getTargetColumns(),
                availableTargets);
        for (String target : targets) {
            if (!labeledTargets.contains(target)) {
                throw new RahaException(RahaErrorCode.INVALID_DATA,
                        "目标字段没有有效直接标签：" + target);
            }
        }
        TrainingMode mode = request.getBaseModelSetVersion() == null
                || request.getBaseModelSetVersion().trim().isEmpty()
                ? TrainingMode.FULL : TrainingMode.INCREMENTAL;
        RahaModelSet parentSet = null;
        List<RahaColumnModel> parentModels = Collections.emptyList();
        List<TrainingExample> parentExamples = Collections.emptyList();
        if (mode == TrainingMode.INCREMENTAL) {
            parentSet = modelStore.findModelSet(request.getBaseModelSetVersion())
                    .orElseThrow(() -> new RahaException(RahaErrorCode.INVALID_REQUEST,
                            "父模型集合不存在：" + request.getBaseModelSetVersion()));
            if (!parentSet.getDatasetId().equals(first.getDatasetId())
                    || !parentSet.getSchemaHash().equals(first.getSchemaHash())) {
                throw new RahaException(RahaErrorCode.INCOMPATIBLE_MODEL,
                        "父模型集合与采样批次不兼容");
            }
            TargetColumnResolver.resolve(targets, parentSet.getModelColumns());
            parentModels = modelStore.loadColumnModels(parentSet.getModelSetVersion());
            parentExamples = modelStore.loadTrainingExamples(parentSet.getModelSetVersion());
        }
        String fingerprint = fingerprint(batchIds, labels, targets, mode,
                request.getBaseModelSetVersion());
        String modelSetVersion = HashUtils.shortId("modelset", fingerprint);
        Optional<RahaModelSet> existing = modelStore.findModelSet(modelSetVersion);
        if (existing.isPresent()) {
            RahaModelSet value = existing.get();
            return new TrainResult(value.getTrainingMode().name(), value.getTrainedColumns(),
                    value.getModelSetVersion(), value.getModelCount(),
                    value.getTrainingExampleCount(), System.currentTimeMillis() - startedAt);
        }
        long createdAt = System.currentTimeMillis();
        String partitionDate = TimeUtils.partitionDate(createdAt,
                config.getPartitionTimeZone());
        List<SampleTuple> tuples = sampleStore.loadTuples(batchIds);
        List<RawTrainingExample> rawExamples = deduplicate(
                datasetBuilder.build(tuples, labels, targets));
        StrategyPlan plan;
        if (mode == TrainingMode.FULL) {
            RahaDataset dataset = datasetLoader.load(first.getInputReference(),
                    first.getDatasetId(), first.getSourceType(), first.getRowKeyColumns(),
                    first.getSnapshotId(), targets);
            try {
                List<ColumnProfile> profiles = columnProfiler.profile(dataset);
                plan = strategyPlanner.plan(profiles);
            } finally {
                dataset.getRows().unpersist(false);
            }
        } else {
            plan = new StrategyPlan(parentSet.getStrategyPlanVersion(),
                    Collections.emptyList());
        }
        Map<String, RahaColumnModel> parentModelByColumn = indexModels(parentModels);
        List<RahaColumnModel> models = new ArrayList<RahaColumnModel>();
        List<TrainingExample> allExamples = new ArrayList<TrainingExample>();
        List<String> successfullyTrained = new ArrayList<String>();
        Set<String> targetSet = new HashSet<String>(targets);
        if (mode == TrainingMode.INCREMENTAL) {
            for (String column : parentSet.getModelColumns()) {
                RahaColumnModel parentModel = parentModelByColumn.get(column);
                List<TrainingExample> copiedParent = copyExamples(parentExamples,
                        column, modelSetVersion, createdAt, partitionDate);
                if (!targetSet.contains(column)) {
                    models.add(copyModel(parentModel, modelSetVersion, createdAt));
                    allExamples.addAll(copiedParent);
                }
            }
        }
        for (String column : targets) {
            List<RawTrainingExample> rawColumn = rawForColumn(rawExamples, column);
            FeatureDictionary dictionary = mode == TrainingMode.INCREMENTAL
                    ? parentModelByColumn.get(column).getFeatureDictionary()
                    : FeatureDictionary.build(column, values(rawColumn),
                    config.getMaximumDictionaryValues());
            List<TrainingExample> current = vectorize(rawColumn, dictionary,
                    modelSetVersion, first.getDatasetId(), createdAt, partitionDate);
            List<TrainingExample> training = current;
            String parentModelVersion = null;
            if (mode == TrainingMode.INCREMENTAL) {
                parentModelVersion = parentModelByColumn.get(column).getModelVersion();
                training = incrementalBuilder.merge(copyExamples(parentExamples, column,
                        modelSetVersion, createdAt, partitionDate), current);
            }
            if (!hasBothClasses(training)) {
                LOGGER.warn("跳过不可训练字段，column={}，examples={}，reason=single_class",
                        column, training.size());
                if (mode == TrainingMode.INCREMENTAL) {
                    models.add(copyModel(parentModelByColumn.get(column),
                            modelSetVersion, createdAt));
                    allExamples.addAll(copyExamples(parentExamples, column,
                            modelSetVersion, createdAt, partitionDate));
                }
                continue;
            }
            RahaColumnModel model = modelTrainer.train(modelSetVersion,
                    first.getDatasetId(), column, parentModelVersion, dictionary,
                    training, createdAt);
            models.add(model);
            successfullyTrained.add(column);
            allExamples.addAll(training);
            LOGGER.info("列模型训练完成，column={}，mode={}，examples={}，modelVersion={}",
                    column, mode, training.size(), model.getModelVersion());
        }
        if (models.isEmpty() || successfullyTrained.isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_DATA,
                    "所有目标字段均不可训练");
        }
        List<String> modelColumns = new ArrayList<String>();
        for (RahaColumnModel model : models) {
            modelColumns.add(model.getColumnName());
        }
        Map<String, Object> configJson = new LinkedHashMap<String, Object>();
        configJson.put("classifierType", "LOGISTIC_REGRESSION");
        configJson.put("maximumDictionaryValues", config.getMaximumDictionaryValues());
        RahaModelSet modelSet = new RahaModelSet(modelSetVersion, fingerprint,
                first.getDatasetId(), first.getSnapshotId(), batchIds, mode,
                request.getBaseModelSetVersion(), modelColumns, successfullyTrained,
                first.getRowIdentityMode(), first.getRowKeyColumns(), first.getSchemaHash(),
                config.getAlgorithmVersion(), JsonUtils.toJson(configJson),
                mode == TrainingMode.FULL ? plan.getVersion()
                        : parentSet.getStrategyPlanVersion(),
                mode == TrainingMode.FULL ? plan.toJson()
                        : parentSet.getStrategyPlanJson(),
                "norm-v1", models.size(), allExamples.size(), createdAt);
        modelStore.save(modelSet, models, allExamples);
        LOGGER.info("训练完成，mode={}，modelSetVersion={}，targetColumns={}，"
                        + "modelCount={}，examples={}，elapsedMillis={}",
                mode, modelSetVersion, targets, models.size(), allExamples.size(),
                System.currentTimeMillis() - startedAt);
        return new TrainResult(mode.name(), targets, modelSetVersion, models.size(),
                allExamples.size(), System.currentTimeMillis() - startedAt);
    }

    private static SampleBatch validateBatches(List<SampleBatch> batches) {
        SampleBatch first = batches.get(0);
        for (SampleBatch batch : batches) {
            if (!first.getDatasetId().equals(batch.getDatasetId())
                    || !first.getSnapshotId().equals(batch.getSnapshotId())
                    || !first.getInputReference().equals(batch.getInputReference())
                    || first.getRowIdentityMode() != batch.getRowIdentityMode()
                    || !first.getRowKeyColumns().equals(batch.getRowKeyColumns())
                    || !first.getSchemaHash().equals(batch.getSchemaHash())) {
                throw new RahaException(RahaErrorCode.INVALID_DATA,
                        "多个采样批次的数据集契约不一致");
            }
        }
        return first;
    }

    private static void validateLabels(List<CellLabel> labels, SampleBatch batch) {
        Map<String, Integer> labelsByCell = new HashMap<String, Integer>();
        for (CellLabel label : labels) {
            if (!batch.getDatasetId().equals(label.getDatasetId())
                    || !batch.getSnapshotId().equals(label.getSnapshotId())) {
                throw new RahaException(RahaErrorCode.INVALID_DATA,
                        "标签数据集或快照与采样批次不一致");
            }
            String key = label.getRowId() + '|' + label.getColumnName();
            Integer previous = labelsByCell.put(key, label.getLabel());
            if (previous != null && previous.intValue() != label.getLabel()) {
                throw new RahaException(RahaErrorCode.INVALID_DATA,
                        "多个采样批次存在冲突标签：" + key);
            }
        }
    }

    private static List<String> targetUnion(List<SampleBatch> batches) {
        LinkedHashSet<String> values = new LinkedHashSet<String>();
        for (SampleBatch batch : batches) {
            values.addAll(batch.getTargetColumns());
        }
        return new ArrayList<String>(values);
    }

    private static List<String> labeledTargets(List<CellLabel> labels,
                                                List<String> available) {
        Set<String> labeled = new HashSet<String>();
        for (CellLabel label : labels) {
            labeled.add(label.getColumnName());
        }
        List<String> result = new ArrayList<String>();
        for (String column : available) {
            if (labeled.contains(column)) {
                result.add(column);
            }
        }
        return result;
    }

    private String fingerprint(List<String> batchIds, List<CellLabel> labels,
                               List<String> targets, TrainingMode mode, String parent) {
        List<String> labelSignatures = new ArrayList<String>();
        for (CellLabel label : labels) {
            labelSignatures.add(label.getSampleBatchId() + '|' + label.getRowId() + '|'
                    + label.getColumnName() + '|' + label.getValueHash() + '|'
                    + label.getLabel());
        }
        Collections.sort(labelSignatures);
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("sampleBatchIds", batchIds);
        values.put("labels", labelSignatures);
        values.put("targetColumns", targets);
        values.put("trainingMode", mode.name());
        values.put("parentModelSetVersion", parent);
        values.put("algorithmVersion", config.getAlgorithmVersion());
        return HashUtils.sha256(JsonUtils.toJson(values));
    }

    private List<TrainingExample> vectorize(List<RawTrainingExample> raw,
                                            FeatureDictionary dictionary,
                                            String modelSetVersion, String datasetId,
                                            long createdAt, String partitionDate) {
        List<TrainingExample> result = new ArrayList<TrainingExample>();
        for (RawTrainingExample example : raw) {
            result.add(new TrainingExample(modelSetVersion, datasetId,
                    example.getSampleBatchId(), example.getColumnName(),
                    example.getSnapshotId(), example.getRowId(),
                    example.getDuplicateCount(), HashUtils.sha256(example.getValue()),
                    vectorizer.vectorize(example.getValue(), dictionary),
                    example.getLabel(), example.getLabelSource(),
                    example.getSampleWeight(), createdAt, partitionDate));
        }
        return result;
    }

    private static List<TrainingExample> copyExamples(List<TrainingExample> parent,
                                                       String column,
                                                       String modelSetVersion,
                                                       long createdAt,
                                                       String partitionDate) {
        List<TrainingExample> result = new ArrayList<TrainingExample>();
        for (TrainingExample example : parent) {
            if (column.equals(example.getColumnName())) {
                result.add(new TrainingExample(modelSetVersion, example.getDatasetId(),
                        example.getSourceSampleBatchId(), example.getColumnName(),
                        example.getSnapshotId(), example.getRowId(),
                        example.getDuplicateCount(), example.getValueHash(),
                        example.getFeatureVector(), example.getLabel(),
                        example.getLabelSource(), example.getSampleWeight(), createdAt,
                        partitionDate));
            }
        }
        return result;
    }

    private static RahaColumnModel copyModel(RahaColumnModel parent,
                                             String modelSetVersion, long createdAt) {
        return new RahaColumnModel(modelSetVersion, parent.getDatasetId(),
                parent.getModelVersion(), parent.getModelVersion(), parent.getColumnName(),
                parent.getFeatureDictionary(), parent.getThreshold(), parent.getIntercept(),
                parent.getCoefficients(), parent.getTrainingSummaryJson(), createdAt);
    }

    private static Map<String, RahaColumnModel> indexModels(List<RahaColumnModel> models) {
        Map<String, RahaColumnModel> result = new LinkedHashMap<String, RahaColumnModel>();
        for (RahaColumnModel model : models) {
            result.put(model.getColumnName(), model);
        }
        return result;
    }

    private static List<RawTrainingExample> rawForColumn(List<RawTrainingExample> values,
                                                         String column) {
        List<RawTrainingExample> result = new ArrayList<RawTrainingExample>();
        for (RawTrainingExample value : values) {
            if (column.equals(value.getColumnName())) {
                result.add(value);
            }
        }
        return result;
    }

    private static List<String> values(List<RawTrainingExample> examples) {
        List<String> result = new ArrayList<String>();
        for (RawTrainingExample example : examples) {
            result.add(example.getValue());
        }
        return result;
    }

    private static boolean hasBothClasses(List<TrainingExample> examples) {
        boolean positive = false;
        boolean negative = false;
        for (TrainingExample example : examples) {
            positive |= example.getLabel() == 1;
            negative |= example.getLabel() == 0;
        }
        return positive && negative;
    }

    private static List<RawTrainingExample> deduplicate(List<RawTrainingExample> values) {
        Map<String, RawTrainingExample> result = new LinkedHashMap<String, RawTrainingExample>();
        for (RawTrainingExample value : values) {
            String key = value.getColumnName() + '|' + value.getRowId();
            RawTrainingExample previous = result.get(key);
            if (previous != null && previous.getLabel() != value.getLabel()) {
                throw new RahaException(RahaErrorCode.INVALID_DATA,
                        "训练样本存在冲突标签：" + key);
            }
            if (previous == null || "DIRECT".equals(value.getLabelSource())) {
                result.put(key, value);
            }
        }
        return new ArrayList<RawTrainingExample>(result.values());
    }

    private static List<String> unique(List<String> values, String label) {
        LinkedHashSet<String> unique = new LinkedHashSet<String>();
        for (String value : values) {
            if (value == null || value.trim().isEmpty() || !unique.add(value)) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                        label + "不能为空或重复");
            }
        }
        return new ArrayList<String>(unique);
    }

    private static void sortBatches(List<SampleBatch> batches, List<String> order) {
        final Map<String, Integer> positions = new HashMap<String, Integer>();
        for (int index = 0; index < order.size(); index++) {
            positions.put(order.get(index), index);
        }
        Collections.sort(batches, new Comparator<SampleBatch>() {
            @Override
            public int compare(SampleBatch left, SampleBatch right) {
                return positions.get(left.getSampleBatchId())
                        .compareTo(positions.get(right.getSampleBatchId()));
            }
        });
    }
}
