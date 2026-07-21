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
     * 获取与任务工厂共享的采样记录仓储。
     *
     * <p>UDF 外层在生成待标注 Excel 时必须读取采样明细，直接复用该仓储可以确保导出内容与训练解析使用同一份
     * c1 批次数据。</p>
     *
     * @return 采样记录仓储
     */
    public SampleRecordRepository getSampleRepository() {
        return sampleRepository;
    }

    /**
     * 获取与任务工厂共享的标注记录仓储。
     *
     * <p>训练 UDF 在 HDFS 未找到上传 Excel 时，会通过该仓储查找已导入的最新可训练人工标注。</p>
     *
     * @return 标注记录仓储
     */
    public AnnotationRecordRepository getAnnotationRepository() {
        return annotationRepository;
    }

    /**
     * 获取与任务工厂共享的模型集合仓储。
     *
     * <p>检测 UDF 的请求工厂会在内部校验模型集合状态，该访问器用于外层报告或诊断场景读取同一份模型集合数据。</p>
     *
     * @return 模型集合仓储
     */
    public ModelSetRepository getModelSetRepository() {
        return modelSetRepository;
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
     * 使用只读 SQL 创建默认首轮采样请求。
     *
     * @param sql FMDB 只读 SQL
     * @return 完整采样执行请求
     */
    public RahaTaskExecutionRequest samplingSql(String sql) {
        return sampling(FmdbInputSpec.sql(sql),
                SamplingRequestOptions.defaults());
    }

    /**
     * 使用只读 SQL 创建默认首轮采样请求，旧数据集参数保留兼容。
     *
     * @param datasetId 旧调用方逻辑数据集标识，新规则按 SQL 首表生成
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
        ExecutionFingerprint fingerprint = inputFingerprint(
                "sampling", input, samplingDiscriminator(options),
                options.getExecutionOverrideOptions());
        RahaJobConfig config = configFactory.jobConfig(JobType.SAMPLING,
                input.getDatasetId(), input.getInputReference(), identity)
                .withExecutionInputFingerprint(
                        fingerprint.getExecutionInputFingerprint());
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
                options.getExistingLabels(), options.getSamplingRound())
                .withExecutionFingerprint(fingerprint);
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
        ExecutionFingerprint batchFingerprint = trainingFingerprint(references,
                trainingInput.spec, options);
        RahaJobConfig config = configFactory.jobConfig(JobType.TRAINING,
                trainingInput.spec.getDatasetId(),
                trainingInput.spec.getInputReference(), trainingInput.identity)
                .withExecutionInputFingerprint(
                        batchFingerprint.getExecutionInputFingerprint());
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
                trainingInput.identity)
                .withExecutionFingerprint(batchFingerprint);
        LOGGER.info("最小训练请求解析完成，datasetId={}，batchCount={}，"
                        + "inputReference={}，identityMode={}",
                trainingInput.spec.getDatasetId(), references.size(),
                trainingInput.spec.getInputReference(),
                trainingInput.identity.getMode());
        return request;
    }

    /**
     * 使用 FMDB 表创建检测请求，自动选择最新完整已发布模型集合。
     *
     * @param inputReference 待检测完整 FMDB 表名
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detectionTable(String inputReference) {
        return detectionTable(inputReference, null);
    }

    /**
     * 使用 FMDB 表和可选不可变模型集合版本创建检测请求。
     *
     * @param inputReference 待检测完整 FMDB 表名
     * @param modelSetVersion 不可变模型集合版本，空值时自动选择最新版本
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detectionTable(
            String inputReference,
            String modelSetVersion) {
        String table = ValueUtils.requireNotBlank(inputReference, "检测 FMDB 表名");
        String version = trimToNull(modelSetVersion);
        ModelSetManifest manifest;
        FmdbInputSpec input;
        if (version == null) {
            input = FmdbInputSpec.table(table);
            manifest = requireLatestPublishedModelSet(input.getDatasetId());
            version = manifest.getModelSetVersion();
        } else {
            manifest = requirePublishedModelSet(version);
            input = new FmdbInputSpec(manifest.getDatasetId(), table,
                    table, DataFormat.FMDB_TABLE, null, null, null,
                    null, null, null, null);
        }
        return detection(input, version,
                DetectionRequestOptions.defaults(), manifest);
    }

    /**
     * 使用只读 SQL 创建检测请求，自动选择最新完整已发布模型集合。
     *
     * @param sql 待检测只读 SQL
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detectionSql(String sql) {
        return detectionSql(sql, null);
    }

    /**
     * 使用只读 SQL 和可选不可变模型集合版本创建检测请求。
     *
     * @param sql 待检测只读 SQL
     * @param modelSetVersion 不可变模型集合版本，空值时自动选择最新版本
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detectionSql(String sql,
            String modelSetVersion) {
        FmdbInputSpec parsed = FmdbInputSpec.sql(sql);
        String version = trimToNull(modelSetVersion);
        ModelSetManifest manifest;
        FmdbInputSpec input;
        if (version == null) {
            input = parsed;
            manifest = requireLatestPublishedModelSet(input.getDatasetId());
            version = manifest.getModelSetVersion();
        } else {
            manifest = requirePublishedModelSet(version);
            input = new FmdbInputSpec(manifest.getDatasetId(),
                    parsed.getInputReference(), parsed.getTableName(),
                    DataFormat.FMDB_SQL, null, parsed.getSnapshotId(),
                    parsed.getSourceVersion(), parsed.getOptions(),
                    parsed.getIncludedColumns(), parsed.getExcludedColumns(),
                    parsed.getSensitiveColumns());
        }
        return detection(input, version,
                DetectionRequestOptions.defaults(), manifest);
    }

    /**
     * 使用只读 SQL 和可选模型集合版本创建检测请求，旧数据集参数保留兼容。
     *
     * @param datasetId 旧调用方逻辑数据集标识，新规则按 SQL 首表生成
     * @param sql 待检测只读 SQL
     * @param modelSetVersion 不可变模型集合版本，空值时自动选择最新版本
     * @return 完整检测执行请求
     */
    public RahaTaskExecutionRequest detectionSql(
            String datasetId,
            String sql,
            String modelSetVersion) {
        if (datasetId != null && !datasetId.trim().isEmpty()) {
            LOGGER.debug("SQL 检测入口忽略外部 datasetId，按首表解析模型来源，"
                    + "datasetId={}", datasetId);
        }
        return detectionSql(sql, modelSetVersion);
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
        if (input == null || options == null) {
            throw new IllegalArgumentException("检测输入和选项不能为空");
        }
        String version = trimToNull(modelSetVersion);
        ModelSetManifest manifest = version == null
                ? requireLatestPublishedModelSet(input.getDatasetId())
                : requirePublishedModelSet(version);
        FmdbInputSpec resolvedInput = version == null ? input
                : rebindDataset(input, manifest.getDatasetId());
        return detection(resolvedInput, manifest.getModelSetVersion(), options,
                manifest);
    }

    /**
     * 基于已解析的模型集清单创建检测任务请求，避免重复查询模型集仓储。
     *
     * <p>该方法会校验检测输入的数据集、行身份规则与模型集是否一致，并将缺失模型策略写入
     * 执行指纹，确保同一输入和同一策略可以稳定复用任务结果。</p>
     *
     * <p>示例：公开入口 {@link #detectionTable(String, String)} 先解析
     * {@code modelSetVersion = "model-set-202607"}，再调用本方法组装检测请求。</p>
     *
     * @param input 检测数据来源，不能为空，数据集必须与模型集一致
     * @param modelSetVersion 模型集不可变版本，用于写入任务请求和执行指纹
     * @param options 检测行为选项，不能为空
     * @param manifest 已发布模型集清单，必须来自同一个版本
     * @return 可交给任务编排器执行的检测请求
     */
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
        ExecutionFingerprint fingerprint = inputFingerprint(
                "detection", input, modelSetVersion + "|"
                        + options.getMissingModelPolicy(),
                options.getExecutionOverrideOptions());
        RahaJobConfig config = configFactory.jobConfig(JobType.DETECTION,
                manifest.getDatasetId(), input.getInputReference(), identity)
                .withExecutionInputFingerprint(
                        fingerprint.getExecutionInputFingerprint());
        if (input.getSnapshotId() != null) {
            config = config.withSnapshotId(input.getSnapshotId());
        }
        LOGGER.info("模型集合检测请求创建完成，datasetId={}，"
                        + "modelSetVersion={}，sourceType={}，missingModelPolicy={}",
                manifest.getDatasetId(), modelSetVersion, input.getFormat(),
                options.getMissingModelPolicy());
        return RahaTaskExecutionRequest.detection(config, loadRequest,
                modelSetVersion, options.getMissingModelPolicy())
                .withExecutionFingerprint(fingerprint);
    }

    /**
     * 从模型集仓储读取并校验已发布模型集。
     *
     * <p>只有已发布的模型集才能进入检测流程，避免检测任务使用未完成或回滚中的模型文件。</p>
     *
     * <p>示例：{@code requirePublishedModelSet("model-set-v1")} 返回状态为已发布的清单；
     * 如果版本不存在或未发布，则抛出异常阻断后续检测。</p>
     *
     * @param modelSetVersion 模型集不可变版本，不能为空
     * @return 已通过发布状态校验的模型集清单
     */
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

    /**
     * 按数据集读取最新完整已发布模型集合。
     *
     * @param datasetId 检测输入解析出的数据集标识
     * @return 最新完整已发布模型集合
     */
    private ModelSetManifest requireLatestPublishedModelSet(String datasetId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "检测数据集标识");
        LOGGER.debug("调用模型集合仓储解析最新检测版本，datasetId={}", dataset);
        Optional<ModelSetManifest> result =
                modelSetRepository.findLatestPublishedByDataset(dataset);
        if (!result.isPresent()) {
            throw new IllegalStateException("数据集没有完整已发布模型集合：" + dataset);
        }
        return result.get().requirePublished();
    }

    private static FmdbInputSpec rebindDataset(FmdbInputSpec input,
                                               String datasetId) {
        if (datasetId.equals(input.getDatasetId())) {
            return input;
        }
        return new FmdbInputSpec(datasetId, input.getInputReference(),
                input.getTableName(), input.getFormat(),
                input.getRowIdentityConfig(), input.getSnapshotId(),
                input.getSourceVersion(), input.getOptions(),
                input.getIncludedColumns(), input.getExcludedColumns(),
                input.getSensitiveColumns());
    }

    /**
     * 从一个或多个训练批次中解析统一的训练数据输入。
     *
     * <p>多批次训练要求数据集、输入来源、来源版本、表结构摘要、行身份规则和来源类型完全一致。
     * 如调用方传入输入覆盖，只允许补充读取方式或版本信息，不能改变采样记录已经确定的数据归属。</p>
     *
     * <p>示例：两个采样批次都来自 {@code ods.user_profile} 且使用相同主键列时，本方法返回
     * 统一的 {@link TrainingInput}；如果其中一个批次来自另一张表，则抛出异常。</p>
     *
     * @param batches 已解析的采样批次与标注批次，至少包含一个批次且每个批次包含采样记录
     * @param inputOverride 可选的训练输入覆盖，用于显式指定读取 SQL 或补充来源版本
     * @return 统一后的训练输入规格和行身份规则
     */
    private static TrainingInput resolveTrainingInput(
            List<ResolvedTrainingBatch> batches,
            FmdbInputSpec inputOverride) {
        SampleRecord first = batches.get(0).sample.getRecords().get(0);
        RowIdentityConfig identity = identity(first);
        String datasetId = first.getDatasetId();
        String inputReference = first.getInputReference();
        String readInputReference = readInputReference(first);
        String sourceVersion = first.getSourceVersion();
        String schemaHash = first.getSchemaHash();
        DataFormat sourceType = sourceType(first);
        for (ResolvedTrainingBatch batch : batches) {
            for (SampleRecord record : batch.sample.getRecords()) {
                if (!datasetId.equals(record.getDatasetId())
                        || !inputReference.equals(record.getInputReference())
                        || !equalsNullable(readInputReference,
                        readInputReference(record))
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
        String tableName = inputReference;
        String loadReference = inputReference;
        if (sourceType == DataFormat.FMDB_SQL) {
            loadReference = readInputReference == null
                    ? inputReference : readInputReference;
            tableName = FmdbSqlSourceTableResolver.firstSourceTable(loadReference);
        }
        FmdbInputSpec spec = new FmdbInputSpec(datasetId, loadReference,
                tableName, sourceType, identity, null, sourceVersion,
                null, null, null, null);
        return new TrainingInput(spec, identity);
    }

    /**
     * 从采样记录还原行身份规则。
     *
     * <p>训练阶段必须沿用采样阶段识别行的方式，否则标签无法可靠映射回原始数据行。</p>
     *
     * <p>示例：采样记录中保存了 {@code rowIdentityMode = PRIMARY_KEY} 和主键列
     * {@code id}，本方法会还原为等价的 {@link RowIdentityConfig}。</p>
     *
     * @param record 采样明细记录，不能为空
     * @return 与采样记录一致的行身份配置
     */
    private static RowIdentityConfig identity(SampleRecord record) {
        return new RowIdentityConfig(record.getRowIdentityMode(),
                record.getRowKeyColumns(), record.getFingerprintAlgorithm(),
                record.getFingerprintVersion());
    }

    /**
     * 校验标注批次是否允许参与训练。
     *
     * <p>默认只允许已导入完成的标注批次参与训练；当允许部分标注时，也接受部分完成状态。
     * 该校验用于防止未完成、废弃或异常状态的标签进入模型训练。</p>
     *
     * <p>示例：{@code allowPartial = false} 时，只有 {@code IMPORTED} 状态合法；
     * {@code allowPartial = true} 时，{@code PARTIAL} 状态也合法。</p>
     *
     * @param annotation 待校验的标注批次
     * @param allowPartial 是否允许部分标注批次参与训练
     */
    private static void validateAnnotationStatus(AnnotationBatch annotation,
                                                 boolean allowPartial) {
        AnnotationBatchStatus status = annotation.getStatus();
        if (status != AnnotationBatchStatus.IMPORTED
                && !(allowPartial && status == AnnotationBatchStatus.PARTIAL)) {
            throw new IllegalArgumentException("标注批次状态不能用于训练：" + status);
        }
    }

    /**
     * 校验训练批次标识并生成稳定排序后的不可变列表。
     *
     * <p>排序后的批次标识会影响训练指纹，必须保证同一组批次无论调用方传入顺序如何，
     * 都能得到相同的执行指纹；重复批次会导致训练样本重复计入，因此直接拒绝。</p>
     *
     * <p>示例：输入 {@code ["batch-b", "batch-a"]}，返回
     * {@code ["batch-a", "batch-b"]}；输入重复的 {@code "batch-a"} 会抛出异常。</p>
     *
     * @param values 调用方传入的采样批次标识列表
     * @return 去重校验后按字典序排列的不可变批次标识列表
     */
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

    /**
     * 从采样上下文中解析原始数据来源类型。
     *
     * <p>该字段由采样落库时写入，用于训练阶段判断应按 FMDB 表还是只读 SQL 重新加载数据。</p>
     *
     * <p>示例：上下文中 {@code sourceType = "FMDB_TABLE"} 时返回
     * {@link DataFormat#FMDB_TABLE}；字段缺失时返回 {@code null}，由上层决定是否需要输入覆盖。</p>
     *
     * @param record 采样明细记录
     * @return 采样来源类型，缺失时返回 {@code null}
     */
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

    private static String readInputReference(SampleRecord record) {
        Object raw = record.getSamplingContext().get(
                SampleRecord.READ_INPUT_REFERENCE_CONTEXT_KEY);
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof String)) {
            throw new IllegalStateException("采样批次读取引用必须为文本");
        }
        String value = ((String) raw).trim();
        return value.isEmpty() ? null : value;
    }

    /**
     * 生成训练任务的稳定输入指纹。
     *
     * <p>指纹由训练批次引用、输入规格、标签传播方法和模型名前缀共同决定。
     * 任一关键输入变化都会生成不同指纹，避免错误复用旧训练结果。</p>
     *
     * <p>示例：同一组批次使用 {@code MAJORITY_VOTE} 与 {@code WEIGHTED_VOTE}
     * 两种传播方法时，会得到两个不同的 SHA-256 指纹。</p>
     *
     * @param references 训练依赖的采样批次和标注批次引用
     * @param input 训练数据输入规格
     * @param options 训练行为选项
     * @return 包含基础指纹和最终指纹的执行指纹
     */
    private static ExecutionFingerprint trainingFingerprint(
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
        return ExecutionFingerprint.fromStableSource(source.toString(),
                options.getExecutionOverrideOptions());
    }

    /**
     * 生成采样或检测任务的稳定输入指纹。
     *
     * <p>该方法将任务类型、规范化输入和额外区分因子拼接后做哈希，
     * 用于任务幂等、结果复用和重复提交识别。</p>
     *
     * <p>示例：采样任务会把标注预算和已有标签摘要作为 {@code discriminator}；
     * 检测任务会把模型集版本和缺失模型策略作为 {@code discriminator}。</p>
     *
     * @param task 任务类型标识，例如 {@code sampling} 或 {@code detection}
     * @param input FMDB 输入规格
     * @param discriminator 任务特有的区分因子
     * @param executionOverrideOptions 执行覆盖选项
     * @return 包含基础指纹和最终指纹的执行指纹
     */
    private static ExecutionFingerprint inputFingerprint(
            String task,
            FmdbInputSpec input,
            String discriminator,
            ExecutionOverrideOptions executionOverrideOptions) {
        StringBuilder source = new StringBuilder();
        appendToken(source, task);
        appendToken(source, canonicalInput(input));
        appendToken(source, discriminator);
        return ExecutionFingerprint.fromStableSource(source.toString(),
                executionOverrideOptions);
    }

    /**
     * 生成采样任务特有的指纹区分因子。
     *
     * <p>已有标签会先按单元格和标签标识排序，再写入所有会影响采样结果的字段，
     * 这样可以避免列表顺序差异造成不必要的重复任务。</p>
     *
     * <p>示例：同一轮采样传入相同标签集合，即使调用方传入顺序不同，
     * 本方法也会返回相同的区分因子。</p>
     *
     * @param options 采样选项，包含采样轮次和已有标签
     * @return 可参与输入指纹计算的稳定字符串
     */
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

    /**
     * 将 FMDB 输入规格规范化为稳定字符串。
     *
     * <p>规范化会固定字段顺序，并对选项、包含列、排除列和敏感列排序，
     * 防止 Map 或 Set 的遍历顺序影响任务指纹。</p>
     *
     * <p>示例：两个输入规格只是在 {@code options} 的插入顺序不同，
     * 本方法仍会产出相同字符串。</p>
     *
     * @param input FMDB 输入规格
     * @return 可用于哈希计算的规范化输入字符串
     */
    private static String canonicalInput(FmdbInputSpec input) {
        StringBuilder source = new StringBuilder();
        appendToken(source, input.getDatasetId());
        appendToken(source, input.getInputReference());
        appendToken(source, input.getSourceReference());
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

    /**
     * 按字典序追加集合内容和集合大小。
     *
     * <p>集合大小会作为边界标记写入，避免不同集合拼接后出现歧义。</p>
     *
     * <p>示例：{@code ["b", "a"]} 会按 {@code "a"}、{@code "b"} 的顺序写入，
     * 最后再写入大小 {@code 2}。</p>
     *
     * @param source 指纹源字符串构建器
     * @param values 待追加的字符串集合
     */
    private static void appendSet(StringBuilder source, Set<String> values) {
        for (String value : new TreeSet<String>(values)) {
            appendToken(source, value);
        }
        appendToken(source, values.size());
    }

    /**
     * 以长度前缀方式追加一个指纹片段。
     *
     * <p>长度前缀可以避免简单字符串拼接产生边界歧义，例如 {@code "ab" + "c"}
     * 与 {@code "a" + "bc"} 不会被编码成相同内容。</p>
     *
     * <p>示例：值 {@code "abc"} 会追加为 {@code "3:abc"}；
     * 空值会以 {@code "<null>"} 参与计算。</p>
     *
     * @param source 指纹源字符串构建器
     * @param value 待追加的值，允许为空
     */
    private static void appendToken(StringBuilder source, Object value) {
        String text = value == null ? "<null>" : String.valueOf(value);
        source.append(text.length()).append(':').append(text);
    }

    /**
     * 判断两个行身份规则是否完全一致。
     *
     * <p>比较范围包括身份模式、主键列、指纹算法和归一化版本。
     * 训练和检测都依赖该判断防止标签或模型映射到错误的数据行。</p>
     *
     * <p>示例：两个配置都使用相同主键列 {@code id} 和相同归一化版本时返回
     * {@code true}；其中任一配置为空时返回 {@code false}。</p>
     *
     * @param first 第一个行身份配置
     * @param second 第二个行身份配置
     * @return 两个配置完全一致时返回 {@code true}
     */
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

    /**
     * 比较两个允许为空的对象是否相等。
     *
     * <p>该工具用于来源版本、来源类型等可选字段，避免直接调用
     * {@link Object#equals(Object)} 时触发空指针异常。</p>
     *
     * <p>示例：两个值都为 {@code null} 时返回 {@code true}；
     * 一个为空、一个不为空时返回 {@code false}。</p>
     *
     * @param first 第一个值
     * @param second 第二个值
     * @return 两个值都为空或内容相等时返回 {@code true}
     */
    private static boolean equalsNullable(Object first, Object second) {
        return first == null ? second == null : first.equals(second);
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    /**
     * 已经完成仓储解析的一组采样和标注批次。
     *
     * <p>该对象只在训练请求组装期间使用，用于把同一个采样批次和最终选中的可训练标注批次绑定在一起。</p>
     *
     * <p>示例：{@code sampleBatchId = "sample-001"} 和其最新可训练标注批次
     * {@code annotationBatchId = "annotation-009"} 会被封装成一个对象。</p>
     */
    private static final class ResolvedTrainingBatch {
        /** 采样批次，包含采样来源、分区月份和采样明细记录。 */
        private final SampleBatch sample;
        /** 选定的标注批次，必须满足训练状态校验。 */
        private final AnnotationBatch annotation;

        private ResolvedTrainingBatch(SampleBatch sample,
                                      AnnotationBatch annotation) {
            this.sample = sample;
            this.annotation = annotation;
        }
    }

    /**
     * 已经确定统一来源和行身份规则的训练输入。
     *
     * <p>该对象是训练任务的数据入口快照，避免后续流程重复推断输入来源和行身份配置。</p>
     *
     * <p>示例：多个采样批次都来自同一张 FMDB 表时，{@code spec} 保存该表读取规格，
     * {@code identity} 保存训练标签回连原始行所需的身份规则。</p>
     */
    private static final class TrainingInput {
        /** FMDB 输入规格，用于训练阶段重新加载完整数据。 */
        private final FmdbInputSpec spec;
        /** 全部训练批次共用的行身份规则，用于稳定匹配样本标签。 */
        private final RowIdentityConfig identity;

        private TrainingInput(FmdbInputSpec spec,
                              RowIdentityConfig identity) {
            this.spec = spec;
            this.identity = identity;
        }
    }
}
