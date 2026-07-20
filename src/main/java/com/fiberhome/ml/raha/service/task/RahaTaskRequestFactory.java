package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.config.validation.RahaConfigFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.domain.ModelSetManifest;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.ModelSetRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import com.fiberhome.ml.raha.service.train.TrainingBatchReference;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将 FMDB 最小参数、持久化批次和模型集合解析为完整任务执行请求。
 *
 * <p>该工厂集中承担默认配置读取和外部仓储查询，请求对象只保存解析完成的不可变结果。</p>
 */
public final class RahaTaskRequestFactory {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RahaTaskRequestFactory.class);
    /** 统一强类型配置工厂。 */
    private final RahaConfigFactory configFactory;
    /** 持久化采样批次仓储。 */
    private final SampleRecordRepository sampleRepository;
    /** 持久化标注批次仓储。 */
    private final AnnotationRecordRepository annotationRepository;
    /** 不可变模型集合仓储。 */
    private final ModelSetRepository modelSetRepository;

    public RahaTaskRequestFactory(
            RahaConfigFactory configFactory,
            SampleRecordRepository sampleRepository,
            AnnotationRecordRepository annotationRepository,
            ModelSetRepository modelSetRepository) {
        if (configFactory == null || sampleRepository == null
                || annotationRepository == null || modelSetRepository == null) {
            throw new IllegalArgumentException("最小任务请求工厂依赖不能为空");
        }
        this.configFactory = configFactory;
        this.sampleRepository = sampleRepository;
        this.annotationRepository = annotationRepository;
        this.modelSetRepository = modelSetRepository;
    }

    /**
     * 使用完整 FMDB 表名创建默认首轮采样请求。
     *
     * @param tableName FMDB 完整表名
     * @return 完整采样执行请求
     */
    public RahaTaskExecutionRequest samplingTable(String tableName) {
        return sampling(FmdbInputSpec.table(tableName),
                SamplingRequestOptions.defaults());
    }

    /**
     * 使用稳定逻辑标识和只读 SQL 创建默认首轮采样请求。
     *
     * @param datasetId 稳定逻辑数据集标识
     * @param sql FMDB 只读 SQL
     * @return 完整采样执行请求
     */
    public RahaTaskExecutionRequest samplingSql(String datasetId, String sql) {
        return sampling(FmdbInputSpec.sql(datasetId, sql),
                SamplingRequestOptions.defaults());
    }

    /**
     * 使用统一输入规格和可选覆盖创建采样请求。
     *
     * @param input FMDB 输入规格
     * @param options 采样覆盖选项
     * @return 完整采样执行请求
     */
    public RahaTaskExecutionRequest sampling(FmdbInputSpec input,
                                             SamplingRequestOptions options) {
        if (input == null || options == null) {
            throw new IllegalArgumentException("采样输入和选项不能为空");
        }
        RowIdentityConfig identity = input.getRowIdentityConfig() == null
                ? RowIdentityConfig.contentHash()
                : input.getRowIdentityConfig();
        DataLoadRequest loadRequest = input.toDataLoadRequest(identity);
        RahaJobConfig config = configFactory.jobConfig(JobType.SAMPLING,
                input.getDatasetId(), input.getInputReference(), identity)
                .withExecutionInputFingerprint(inputFingerprint(
                        "sampling", input, samplingDiscriminator(options)));
        if (input.getSnapshotId() != null) {
            config = config.withSnapshotId(input.getSnapshotId());
        }
        if (options.getLabelingBudget() != null) {
            SamplingConfig sampling = config.getSamplingConfig()
                    .withLabelingBudget(options.getLabelingBudget());
            config = config.withSamplingConfig(sampling);
        }
        LOGGER.info("最小采样请求创建完成，datasetId={}，sourceType={}，"
                        + "samplingRound={}，labelingBudget={}",
                input.getDatasetId(), input.getFormat(),
                options.getSamplingRound(),
                config.getSamplingConfig().getLabelingBudget());
        return RahaTaskExecutionRequest.sampling(config, loadRequest,
                options.getExistingLabels(), options.getSamplingRound());
    }

    /**
     * 使用单个全局采样批次标识创建默认训练请求。
     *
     * @param sampleBatchId 全局唯一采样批次标识
     * @return 完整持久化批次训练请求
     */
    public RahaTaskExecutionRequest training(String sampleBatchId) {
        return training(Collections.singletonList(sampleBatchId),
                TrainingRequestOptions.defaults());
    }

    /**
     * 解析一个或多个采样批次及其最新有效标注，创建确定性训练请求。
     *
     * @param sampleBatchIds 全局唯一采样批次标识
     * @param options 标注选择和训练覆盖选项
     * @return 完整多批次训练执行请求
     */
    public RahaTaskExecutionRequest training(
            List<String> sampleBatchIds,
            TrainingRequestOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("训练请求选项不能为空");
        }
        List<String> batchIds = sortedUniqueBatchIds(sampleBatchIds);
        LOGGER.info("开始解析最小训练请求，sampleBatchCount={}，allowPartial={}",
                batchIds.size(), options.isAllowPartialAnnotation());
        List<ResolvedTrainingBatch> resolved =
                new ArrayList<ResolvedTrainingBatch>(batchIds.size());
        for (String batchId : batchIds) {
            LOGGER.debug("调用采样仓储解析训练批次，sampleBatchId={}", batchId);
            SampleBatch sample = sampleRepository.findByBatchId(batchId)
                    .orElseThrow(() -> new IllegalStateException(
                            "训练采样批次不存在：" + batchId));
            LOGGER.debug("调用标注仓储选择训练标注，sampleBatchId={}", batchId);
            AnnotationBatch annotation = annotationRepository
                    .findLatestTrainableForSample(batchId,
                            options.isAllowPartialAnnotation())
                    .orElseThrow(() -> new IllegalStateException(
                            "采样批次没有可用于训练的标注：" + batchId));
            validateAnnotationStatus(annotation,
                    options.isAllowPartialAnnotation());
            if (!sample.getDatasetId().equals(annotation.getDatasetId())
                    || !sample.getSampleBatchId().equals(
                    annotation.getSampleBatchId())) {
                throw new IllegalArgumentException("采样批次与标注批次归属不一致："
                        + batchId);
            }
            resolved.add(new ResolvedTrainingBatch(sample, annotation));
        }
        TrainingInput trainingInput = resolveTrainingInput(resolved,
                options.getInputOverride());
        List<TrainingBatchReference> references =
                new ArrayList<TrainingBatchReference>(resolved.size());
        for (ResolvedTrainingBatch item : resolved) {
            references.add(new TrainingBatchReference(
                    item.sample.getSampleBatchId(),
                    item.sample.getPartitionMonth(),
                    item.annotation.getAnnotationBatchId(),
                    item.annotation.getPartitionMonth()));
        }
        String batchFingerprint = trainingFingerprint(references,
                trainingInput.spec, options);
        RahaJobConfig config = configFactory.jobConfig(JobType.TRAINING,
                trainingInput.spec.getDatasetId(),
                trainingInput.spec.getInputReference(), trainingInput.identity)
                .withExecutionInputFingerprint(batchFingerprint);
        if (trainingInput.spec.getSnapshotId() != null) {
            config = config.withSnapshotId(trainingInput.spec.getSnapshotId());
        }
        DataLoadRequest loadRequest = trainingInput.spec.toDataLoadRequest(
                trainingInput.identity);
        RahaTaskExecutionRequest request = RahaTaskExecutionRequest.trainingBatches(
                config, loadRequest, options.getPropagationMethod(),
                configFactory.labelPropagationConfig(),
                configFactory.logisticRegressionTrainingConfig(),
                options.getModelNamePrefix(), references,
                trainingInput.identity);
        LOGGER.info("最小训练请求解析完成，datasetId={}，batchCount={}，"
                        + "inputReference={}，identityMode={}",
                trainingInput.spec.getDatasetId(), references.size(),
                trainingInput.spec.getInputReference(),
                trainingInput.identity.getMode());
        return request;
    }

    /**
     * 使用 FMDB 表和不可变模型集合版本创建检测请求。
     *
     * @param inputReference 待检测完整 FMDB 表名
     * @param modelSetVersion 不可变模型集合版本
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detectionTable(
            String inputReference,
            String modelSetVersion) {
        ModelSetManifest manifest = requirePublishedModelSet(modelSetVersion);
        String table = ValueUtils.requireNotBlank(inputReference, "检测 FMDB 表名");
        FmdbInputSpec input = new FmdbInputSpec(manifest.getDatasetId(), table,
                table, DataFormat.FMDB_TABLE, null, null, null,
                null, null, null, null);
        return detection(input, modelSetVersion,
                DetectionRequestOptions.defaults(), manifest);
    }

    /**
     * 使用只读 SQL 和不可变模型集合版本创建检测请求。
     *
     * @param datasetId 调用方声明的逻辑数据集标识
     * @param sql 待检测只读 SQL
     * @param modelSetVersion 不可变模型集合版本
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detectionSql(
            String datasetId,
            String sql,
            String modelSetVersion) {
        ModelSetManifest manifest = requirePublishedModelSet(modelSetVersion);
        FmdbInputSpec input = FmdbInputSpec.sql(datasetId, sql);
        return detection(input, modelSetVersion,
                DetectionRequestOptions.defaults(), manifest);
    }

    /**
     * 使用统一输入规格、模型集合和缺失模型策略创建检测请求。
     *
     * @param input FMDB 检测输入
     * @param modelSetVersion 不可变模型集合版本
     * @param options 检测行为选项
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detection(
            FmdbInputSpec input,
            String modelSetVersion,
            DetectionRequestOptions options) {
        return detection(input, modelSetVersion, options,
                requirePublishedModelSet(modelSetVersion));
    }

    private RahaTaskExecutionRequest detection(
            FmdbInputSpec input,
            String modelSetVersion,
            DetectionRequestOptions options,
            ModelSetManifest manifest) {
        if (input == null || options == null) {
            throw new IllegalArgumentException("检测输入和选项不能为空");
        }
        if (!manifest.getDatasetId().equals(input.getDatasetId())) {
            throw new IllegalArgumentException("检测逻辑数据集与模型集合不一致");
        }
        RowIdentityConfig identity = manifest.getRowIdentityConfig();
        if (input.getRowIdentityConfig() != null
                && !sameIdentity(identity, input.getRowIdentityConfig())) {
            throw new IllegalArgumentException("检测行身份覆盖与模型集合不兼容");
        }
        DataLoadRequest loadRequest = input.toDataLoadRequest(identity);
        RahaJobConfig config = configFactory.jobConfig(JobType.DETECTION,
                manifest.getDatasetId(), input.getInputReference(), identity)
                .withExecutionInputFingerprint(inputFingerprint(
                        "detection", input, modelSetVersion + "|"
                                + options.getMissingModelPolicy()));
        if (input.getSnapshotId() != null) {
            config = config.withSnapshotId(input.getSnapshotId());
        }
        LOGGER.info("显式模型集合检测请求创建完成，datasetId={}，"
                        + "modelSetVersion={}，sourceType={}，missingModelPolicy={}",
                manifest.getDatasetId(), modelSetVersion, input.getFormat(),
                options.getMissingModelPolicy());
        return RahaTaskExecutionRequest.detection(config, loadRequest,
                modelSetVersion, options.getMissingModelPolicy());
    }

    private ModelSetManifest requirePublishedModelSet(String modelSetVersion) {
        String version = ValueUtils.requireNotBlank(
                modelSetVersion, "检测模型集合版本");
        LOGGER.debug("调用模型集合仓储解析检测版本，modelSetVersion={}", version);
        Optional<ModelSetManifest> result = modelSetRepository.find(version);
        if (!result.isPresent()) {
            throw new IllegalStateException("模型集合不存在：" + version);
        }
        return result.get().requirePublished();
    }

    private static TrainingInput resolveTrainingInput(
            List<ResolvedTrainingBatch> batches,
            FmdbInputSpec inputOverride) {
        SampleRecord first = batches.get(0).sample.getRecords().get(0);
        RowIdentityConfig identity = identity(first);
        String datasetId = first.getDatasetId();
        String inputReference = first.getInputReference();
        String sourceVersion = first.getSourceVersion();
        String schemaHash = first.getSchemaHash();
        DataFormat sourceType = sourceType(first);
        for (ResolvedTrainingBatch batch : batches) {
            for (SampleRecord record : batch.sample.getRecords()) {
                if (!datasetId.equals(record.getDatasetId())
                        || !inputReference.equals(record.getInputReference())
                        || !equalsNullable(sourceVersion, record.getSourceVersion())
                        || !schemaHash.equals(record.getSchemaHash())
                        || !sameIdentity(identity, identity(record))
                        || !equalsNullable(sourceType, sourceType(record))) {
                    throw new IllegalArgumentException(
                            "多批次训练的数据集、来源类型、模式或行身份不一致");
                }
            }
        }
        if (inputOverride != null) {
            if (!datasetId.equals(inputOverride.getDatasetId())) {
                throw new IllegalArgumentException("训练输入覆盖不能改变逻辑数据集");
            }
            RowIdentityConfig overrideIdentity = inputOverride.getRowIdentityConfig();
            if (overrideIdentity != null && !sameIdentity(identity, overrideIdentity)) {
                throw new IllegalArgumentException("训练输入覆盖不能改变行身份规则");
            }
            if (sourceType != null && sourceType != inputOverride.getFormat()) {
                throw new IllegalArgumentException("训练输入覆盖不能改变已记录的来源类型");
            }
            FmdbInputSpec resolvedOverride = inputOverride.getSourceVersion() == null
                    && sourceVersion != null
                    ? inputOverride.withVersion(inputOverride.getSnapshotId(),
                    sourceVersion)
                    : inputOverride;
            return new TrainingInput(resolvedOverride, identity);
        }
        if (sourceType != DataFormat.FMDB_TABLE
                && sourceType != DataFormat.FMDB_SQL) {
            throw new IllegalStateException(sourceType == null
                    ? "采样批次缺少来源类型，请显式提供训练输入覆盖"
                    : "最小 FMDB 训练入口不支持采样来源类型：" + sourceType);
        }
        String tableName = sourceType == DataFormat.FMDB_TABLE
                ? inputReference : datasetId;
        FmdbInputSpec spec = new FmdbInputSpec(datasetId, inputReference,
                tableName, sourceType, identity, null, sourceVersion,
                null, null, null, null);
        return new TrainingInput(spec, identity);
    }

    private static RowIdentityConfig identity(SampleRecord record) {
        return new RowIdentityConfig(record.getRowIdentityMode(),
                record.getRowKeyColumns(), record.getFingerprintAlgorithm(),
                record.getFingerprintVersion());
    }

    private static void validateAnnotationStatus(AnnotationBatch annotation,
                                                 boolean allowPartial) {
        AnnotationBatchStatus status = annotation.getStatus();
        if (status != AnnotationBatchStatus.IMPORTED
                && !(allowPartial && status == AnnotationBatchStatus.PARTIAL)) {
            throw new IllegalArgumentException("标注批次状态不能用于训练：" + status);
        }
    }

    private static List<String> sortedUniqueBatchIds(List<String> values) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("训练采样批次标识不能为空");
        }
        Set<String> unique = new LinkedHashSet<String>();
        for (String value : values) {
            String batchId = ValueUtils.requireNotBlank(value, "训练采样批次标识");
            if (!unique.add(batchId)) {
                throw new IllegalArgumentException("训练采样批次标识不能重复："
                        + batchId);
            }
        }
        List<String> result = new ArrayList<String>(unique);
        Collections.sort(result);
        return Collections.unmodifiableList(result);
    }

    private static DataFormat sourceType(SampleRecord record) {
        Object raw = record.getSamplingContext().get(
                SampleRecord.SOURCE_TYPE_CONTEXT_KEY);
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof String)) {
            throw new IllegalStateException("采样批次来源类型必须为文本");
        }
        try {
            return DataFormat.valueOf((String) raw);
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException("采样批次来源类型非法：" + raw,
                    exception);
        }
    }

    private static String trainingFingerprint(
            List<TrainingBatchReference> references,
            FmdbInputSpec input,
            TrainingRequestOptions options) {
        StringBuilder source = new StringBuilder("training");
        for (TrainingBatchReference reference : references) {
            appendToken(source, reference.toCanonicalString());
        }
        appendToken(source, canonicalInput(input));
        appendToken(source, options.getPropagationMethod());
        appendToken(source, options.getModelNamePrefix());
        return HashUtils.sha256Hex(source.toString());
    }

    private static String inputFingerprint(String task,
                                           FmdbInputSpec input,
                                           String discriminator) {
        StringBuilder source = new StringBuilder();
        appendToken(source, task);
        appendToken(source, canonicalInput(input));
        appendToken(source, discriminator);
        return HashUtils.sha256Hex(source.toString());
    }

    private static String samplingDiscriminator(
            SamplingRequestOptions options) {
        StringBuilder source = new StringBuilder();
        appendToken(source, options.getSamplingRound());
        List<CellLabel> labels = new ArrayList<CellLabel>(
                options.getExistingLabels());
        Collections.sort(labels, new Comparator<CellLabel>() {
            @Override
            public int compare(CellLabel first, CellLabel second) {
                int byCell = first.getCellId().compareTo(second.getCellId());
                return byCell != 0 ? byCell
                        : first.getLabelId().compareTo(second.getLabelId());
            }
        });
        for (CellLabel label : labels) {
            appendToken(source, label.getLabelId());
            appendToken(source, label.getCellId());
            appendToken(source, label.getLabel());
            appendToken(source, label.getLabelSource());
            appendToken(source, label.getConfidence());
            appendToken(source, label.getSourceLabelId());
            appendToken(source, label.getClusterId());
            appendToken(source, label.getClusterVersion());
            appendToken(source, label.getPropagationMethod());
            appendToken(source, label.getSampleWeight());
            appendToken(source, label.getConflictCount());
            appendToken(source, label.getMajorityRatio());
        }
        return source.toString();
    }

    private static String canonicalInput(FmdbInputSpec input) {
        StringBuilder source = new StringBuilder();
        appendToken(source, input.getDatasetId());
        appendToken(source, input.getInputReference());
        appendToken(source, input.getTableName());
        appendToken(source, input.getFormat());
        appendToken(source, input.getRowIdentityConfig() == null ? null
                : input.getRowIdentityConfig().toCanonicalString());
        appendToken(source, input.getSnapshotId());
        appendToken(source, input.getSourceVersion());
        appendToken(source, "options");
        for (Map.Entry<String, String> entry
                : new TreeMap<String, String>(input.getOptions()).entrySet()) {
            appendToken(source, entry.getKey());
            appendToken(source, entry.getValue());
        }
        appendToken(source, input.getOptions().size());
        appendToken(source, "includedColumns");
        appendSet(source, input.getIncludedColumns());
        appendToken(source, "excludedColumns");
        appendSet(source, input.getExcludedColumns());
        appendToken(source, "sensitiveColumns");
        appendSet(source, input.getSensitiveColumns());
        return source.toString();
    }

    private static void appendSet(StringBuilder source, Set<String> values) {
        for (String value : new TreeSet<String>(values)) {
            appendToken(source, value);
        }
        appendToken(source, values.size());
    }

    private static void appendToken(StringBuilder source, Object value) {
        String text = value == null ? "<null>" : String.valueOf(value);
        source.append(text.length()).append(':').append(text);
    }

    private static boolean sameIdentity(RowIdentityConfig first,
                                        RowIdentityConfig second) {
        return first != null && second != null
                && first.getMode() == second.getMode()
                && first.getKeyColumns().equals(second.getKeyColumns())
                && first.getFingerprintAlgorithm()
                == second.getFingerprintAlgorithm()
                && first.getNormalizationVersion().equals(
                second.getNormalizationVersion());
    }

    private static boolean equalsNullable(Object first, Object second) {
        return first == null ? second == null : first.equals(second);
    }

    /** 已经完成仓储解析的一组采样和标注批次。 */
    private static final class ResolvedTrainingBatch {
        /** 采样批次。 */
        private final SampleBatch sample;
        /** 选定标注批次。 */
        private final AnnotationBatch annotation;

        private ResolvedTrainingBatch(SampleBatch sample,
                                      AnnotationBatch annotation) {
            this.sample = sample;
            this.annotation = annotation;
        }
    }

    /** 已经确定统一来源和行身份规则的训练输入。 */
    private static final class TrainingInput {
        /** FMDB 输入规格。 */
        private final FmdbInputSpec spec;
        /** 全部训练批次共用的行身份规则。 */
        private final RowIdentityConfig identity;

        private TrainingInput(FmdbInputSpec spec,
                              RowIdentityConfig identity) {
            this.spec = spec;
            this.identity = identity;
        }
    }
}
