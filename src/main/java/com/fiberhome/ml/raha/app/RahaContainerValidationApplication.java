package com.fiberhome.ml.raha.app;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.ScalableColumnClusterer;
import com.fiberhome.ml.raha.config.RahaConfigFactory;
import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.RahaJobConfig;
import com.fiberhome.ml.raha.config.ClusteringConfig;
import com.fiberhome.ml.raha.data.JobType;
import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.loader.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.evaluation.DetectionEvaluationMetrics;
import com.fiberhome.ml.raha.evaluation.DetectionEvaluationService;
import com.fiberhome.ml.raha.evaluation.CellScore;
import com.fiberhome.ml.raha.evaluation.EvaluationSplit;
import com.fiberhome.ml.raha.evaluation.EvaluationSplitService;
import com.fiberhome.ml.raha.evaluation.GroundTruthDifferenceResult;
import com.fiberhome.ml.raha.evaluation.GroundTruthDifferenceService;
import com.fiberhome.ml.raha.evaluation.ThresholdComparisonResult;
import com.fiberhome.ml.raha.evaluation.ThresholdComparisonService;
import com.fiberhome.ml.raha.evaluation.ThresholdSelectionPolicy;
import com.fiberhome.ml.raha.feature.FeatureAssembler;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.fmdb.DefaultFmdbSchemaResolver;
import com.fiberhome.ml.raha.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.fmdb.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.fmdb.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.label.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.AdaptiveColumnModelTrainer;
import com.fiberhome.ml.raha.model.ColumnModelCompatibilityValidator;
import com.fiberhome.ml.raha.model.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.ColumnModelMetadataFactory;
import com.fiberhome.ml.raha.model.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.ColumnModelVersioner;
import com.fiberhome.ml.raha.model.ColumnTrainingDataBuilder;
import com.fiberhome.ml.raha.model.ModelReleaseManager;
import com.fiberhome.ml.raha.model.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.model.SparkMllibLogisticRegressionTrainer;
import com.fiberhome.ml.raha.model.WeightedRuleFallbackTrainer;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultCellLabelRepository;
import com.fiberhome.ml.raha.repository.DefaultAnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.StrategyRepository;
import com.fiberhome.ml.raha.service.RahaDetectOutput;
import com.fiberhome.ml.raha.service.RahaDetectRequest;
import com.fiberhome.ml.raha.service.RahaDetectService;
import com.fiberhome.ml.raha.service.RahaTaskResult;
import com.fiberhome.ml.raha.service.RahaTaskStatus;
import com.fiberhome.ml.raha.service.RahaTaskSummary;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.service.RahaTrainOutput;
import com.fiberhome.ml.raha.service.RahaTrainRequest;
import com.fiberhome.ml.raha.service.RahaTrainService;
import com.fiberhome.ml.raha.service.ActiveSamplingOrchestrator;
import com.fiberhome.ml.raha.service.ActiveSamplingResult;
import com.fiberhome.ml.raha.service.RahaFeaturePreparationRequest;
import com.fiberhome.ml.raha.service.RahaFeaturePreparationResult;
import com.fiberhome.ml.raha.service.RahaFeaturePreparationService;
import com.fiberhome.ml.raha.service.RahaSampleService;
import com.fiberhome.ml.raha.service.SampledTupleLabelProvider;
import com.fiberhome.ml.raha.sampling.AnnotationTask;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.SamplingService;
import com.fiberhome.ml.raha.sampling.SamplingVersioner;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.strategy.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.StrategyAlignmentArtifactWriter;
import com.fiberhome.ml.raha.strategy.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.StrategyPlanService;
import com.fiberhome.ml.raha.strategy.StrategyRegistry;
import com.fiberhome.ml.raha.udf.F_DW_RAHADETECT;
import com.fiberhome.ml.raha.udf.F_DW_RAHASAMPLE;
import com.fiberhome.ml.raha.udf.F_DW_RAHATRAIN;
import com.fiberhome.ml.raha.udf.RahaDetectUdfRequest;
import com.fiberhome.ml.raha.udf.RahaDetectUdfHandler;
import com.fiberhome.ml.raha.udf.RahaSampleUdfRequest;
import com.fiberhome.ml.raha.udf.RahaSampleUdfHandler;
import com.fiberhome.ml.raha.udf.RahaTrainUdfRequest;
import com.fiberhome.ml.raha.udf.RahaTrainUdfHandler;
import com.fiberhome.ml.raha.udf.RahaUdfExecutionResult;
import com.fiberhome.ml.raha.udf.RahaUdfRegistrar;
import com.fiberhome.ml.raha.util.FormDataCodec;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 在 Spark 集群中使用样例脏表和真值表完成 UDF 直接采样、训练、检测与评估验收。
 */
public final class RahaContainerValidationApplication {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RahaContainerValidationApplication.class);
    /** 当前验收数据集标识，可通过系统属性覆盖。 */
    private static final String DATASET_ID = System.getProperty(
            "fmdb.validation.dataset-id", "raha-toy");
    /** 当前验收数据快照标识，可通过系统属性覆盖。 */
    private static final String SNAPSHOT_ID = System.getProperty(
            "fmdb.validation.snapshot-id", DATASET_ID + "-snapshot-v1");
    /** 可安全用于 Spark 表名的数据集标识。 */
    private static final String TABLE_DATASET_ID = tableToken(DATASET_ID);
    /** 脏表临时视图。 */
    private static final String DIRTY_TABLE = "raha_validation_" + TABLE_DATASET_ID + "_dirty";
    /** 真值表临时视图。 */
    private static final String CLEAN_TABLE = "raha_validation_" + TABLE_DATASET_ID + "_clean";
    /** 模型产物临时表。 */
    private static final String MODEL_TABLE = "raha_validation_" + TABLE_DATASET_ID + "_models";
    /** 特征字典临时表。 */
    private static final String DICTIONARY_TABLE = "raha_validation_"
            + TABLE_DATASET_ID + "_dictionaries";
    /** 检测结果临时表。 */
    private static final String RESULT_TABLE = "raha_validation_" + TABLE_DATASET_ID
            + "_detection_results";
    /** 当前验收稳定行标识字段，可通过系统属性覆盖。 */
    private static final String ROW_ID_COLUMN = System.getProperty(
            "fmdb.validation.row-id-column", "ID");
    /** 当前训练任务标识。 */
    private static final String TRAIN_JOB_ID = DATASET_ID + "-train-job";
    /** 当前采样任务标识。 */
    private static final String SAMPLE_JOB_ID = DATASET_ID + "-sample-job";
    /** 当前检测任务标识。 */
    private static final String DETECT_JOB_ID = DATASET_ID + "-detect-job";
    /** 当前配置版本。 */
    private final String configVersion;
    /** 当前系统时钟。 */
    private final Clock clock;
    /** Spark 运行会话。 */
    private final SparkSession spark;
    /** 脏数据文件路径。 */
    private final String dirtyPath;
    /** 真值数据文件路径。 */
    private final String cleanPath;
    /** 验收产物输出目录。 */
    private final Path outputDirectory;
    /** 统一配置对象工厂。 */
    private final RahaConfigFactory configFactory;

    private RahaContainerValidationApplication(SparkSession spark,
                                                String dirtyPath,
                                                String cleanPath,
                                                String outputDirectory) {
        if (spark == null) {
            throw new IllegalArgumentException("Spark 会话不能为空");
        }
        this.spark = spark;
        this.dirtyPath = sparkPath(dirtyPath);
        this.cleanPath = sparkPath(cleanPath);
        this.outputDirectory = Paths.get(outputDirectory).toAbsolutePath().normalize();
        this.clock = Clock.systemUTC();
        this.configFactory = RahaDefaultConfigProvider.factory();
        this.configVersion = configFactory.executionFingerprint();
    }

    /**
     * 启动容器验收任务。
     *
     * @param args 脏表路径、真值表路径和输出目录
     */
    public static void main(String[] args) {
        if (args == null || args.length != 3) {
            throw new IllegalArgumentException("参数必须依次为脏表路径、真值表路径和输出目录");
        }
        SparkSession spark = SparkSession.builder()
                .appName("Raha 容器训练检测评估验收")
                .getOrCreate();
        try {
            new RahaContainerValidationApplication(spark, args[0], args[1], args[2])
                    .run();
        } catch (RuntimeException exception) {
            LOGGER.error("Raha 容器训练检测评估验收失败", exception);
            throw exception;
        } finally {
            spark.stop();
        }
    }

    /**
     * 串联三个同步 UDF 和生产服务，输出可复核的检测结果与评估摘要。
     */
    private void run() {
        runDirectLifecycle();
    }

    /**
     * 在当前进程按采样、训练、检测顺序同步执行三个 UDF。
     */
    private void runDirectLifecycle() {
        long startedAt = System.currentTimeMillis();
        LOGGER.info("开始 Raha 容器同步 UDF 验收，dirtyPath={}，cleanPath={}，"
                        + "outputDirectory={}",
                dirtyPath, cleanPath, outputDirectory);
        try {
            prepareOutputDirectory();
            loadCsvView(dirtyPath, DIRTY_TABLE);
            loadCsvView(cleanPath, CLEAN_TABLE);
            long dirtyRowCount = spark.table(DIRTY_TABLE).count();
            long cleanRowCount = spark.table(CLEAN_TABLE).count();
            if (dirtyRowCount != cleanRowCount || dirtyRowCount == 0L) {
                throw new IllegalStateException("脏表和真值表行数必须相等且大于零");
            }

            InMemoryFmdbTableGateway fmdbGateway = new InMemoryFmdbTableGateway(spark);
            InMemoryRahaRepository coreStorage = new InMemoryRahaRepository();
            RahaDataset dirtyDataset = profile(loadDataset(DIRTY_TABLE, DATASET_ID,
                    SNAPSHOT_ID), coreStorage);
            RahaDataset cleanDataset = loadDataset(CLEAN_TABLE, DATASET_ID + "-truth",
                    SNAPSHOT_ID);
            GroundTruthDifferenceResult truth = new GroundTruthDifferenceService(
                    new DefaultCellLabelRepository(coreStorage), clock).compareAndSave(
                    DATASET_ID + "-evaluation", dirtyDataset, cleanDataset,
                    version("ground-truth-stage"));

            AtomicReference<SampledLabels> sampledReference =
                    new AtomicReference<SampledLabels>();
            AtomicReference<TrainingContext> trainingReference =
                    new AtomicReference<TrainingContext>();
            AtomicReference<RahaTaskResult<RahaDetectOutput>> detectedReference =
                    new AtomicReference<RahaTaskResult<RahaDetectOutput>>();
            RahaSampleUdfHandler sampleHandler = request -> executeSampleUdfTask(
                    request, dirtyDataset, truth, coreStorage, sampledReference);
            RahaTrainUdfHandler trainHandler = request -> executeTrainUdfTask(
                    request, dirtyDataset, coreStorage, fmdbGateway,
                    sampledReference, trainingReference);
            RahaDetectUdfHandler detectHandler = request -> executeDetectUdfTask(
                    request, dirtyDataset, coreStorage, fmdbGateway,
                    trainingReference, detectedReference);
            new RahaUdfRegistrar().register(spark, sampleHandler, trainHandler,
                    detectHandler);

            F_DW_RAHASAMPLE sampleUdf = new F_DW_RAHASAMPLE(sampleHandler);
            F_DW_RAHATRAIN trainUdf = new F_DW_RAHATRAIN(trainHandler);
            F_DW_RAHADETECT detectUdf = new F_DW_RAHADETECT(detectHandler);
            String sampleUdfResult = callSampleUdf(sampleUdf, sampleUdfRequest());
            requireCompleted("采样", sampleUdfResult);
            String trainUdfResult = callTrainUdf(trainUdf, trainUdfRequest());
            requireCompleted("训练", trainUdfResult);
            String detectUdfResult = callDetectUdf(detectUdf, detectUdfRequest(
                    PublishedColumnModelLoader.CURRENT_PUBLISHED_VERSION));
            requireCompleted("检测", detectUdfResult);
            SampledLabels sampledLabels = requireValue(
                    "采样", sampledReference.get());
            TrainingContext training = requireValue(
                    "训练", trainingReference.get());
            RahaTaskResult<RahaDetectOutput> detected = requireValue(
                    "检测", detectedReference.get());
            DetectionEvaluationMetrics fullMetrics = new DetectionEvaluationService()
                    .evaluate(detected.getPayload().getResults(), truth.getLabels());
            EvaluationSlice holdout = evaluationSlice(
                    detected.getPayload().getResults(),
                    sampledLabels.evaluationSplit.getTestLabels());
            DetectionEvaluationMetrics metrics = new DetectionEvaluationService()
                    .evaluate(holdout.detections, holdout.labels);
            long resultCount = writeDetectionResults(detected, fmdbGateway);
            writeSummary(dirtyRowCount, truth, training, detected, resultCount,
                    metrics, fullMetrics, sampledLabels, trainUdfResult, detectUdfResult,
                    sampleUdfResult,
                    System.currentTimeMillis() - startedAt);
            LOGGER.info("Raha 容器同步 UDF 验收完成，rowCount={}，truthPositiveCount={}，"
                            + "detectedResultCount={}，precision={}，recall={}，f1={}，"
                            + "directLabelRowCount={}，elapsedMillis={}",
                    dirtyRowCount, truth.getPositiveCount(), resultCount,
                    metrics.getPrecision(), metrics.getRecall(), metrics.getF1(),
                    sampledLabels.rowIds.size(), System.currentTimeMillis() - startedAt);
        } catch (RuntimeException exception) {
            // 文件、Spark、FMDB 适配或算法任一环节失败时保留完整上下文和堆栈。
            LOGGER.error("Raha 容器验收核心流程失败，dirtyPath={}，cleanPath={}",
                    dirtyPath, cleanPath, exception);
            throw exception;
        }
    }

    private void prepareOutputDirectory() {
        try {
            LOGGER.info("开始准备验收文件目录，outputDirectory={}", outputDirectory);
            Files.createDirectories(outputDirectory);
        } catch (java.io.IOException exception) {
            LOGGER.error("准备验收文件目录失败，outputDirectory={}",
                    outputDirectory, exception);
            throw new IllegalStateException("无法准备验收文件目录", exception);
        }
    }

    private void loadCsvView(String path, String tableName) {
        LOGGER.info("开始通过 Spark 读取 CSV，path={}，tableName={}", path, tableName);
        try {
            spark.read().option("header", "true")
                    .option("inferSchema", "false")
                    .option("mode", "FAILFAST")
                    .csv(path)
                    .createOrReplaceTempView(tableName);
            LOGGER.info("Spark CSV 临时视图创建完成，path={}，tableName={}", path, tableName);
        } catch (RuntimeException exception) {
            LOGGER.error("Spark CSV 读取失败，path={}，tableName={}",
                    path, tableName, exception);
            throw exception;
        }
    }

    private RahaDataset loadDataset(String tableName,
                                    String datasetId,
                                    String snapshotId) {
        FmdbDatasetLoader loader = new FmdbDatasetLoader(spark,
                new RowIdValidator(), new SchemaHasher(),
                new DefaultFmdbSchemaResolver(new ColumnMetadataFactory()),
                new SnapshotMetadataFactory(), clock);
        DataLoadRequest request = new DataLoadRequest(datasetId, tableName,
                tableName, ROW_ID_COLUMN, DataFormat.FMDB_TABLE,
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), snapshotId, DATASET_ID + "-csv-v1");
        return loader.load(request).getDataset();
    }

    private RahaDataset profile(RahaDataset dataset,
                                InMemoryRahaRepository storage) {
        return new ColumnProfileService(new ColumnProfiler(),
                new DefaultColumnProfileRepository(storage), clock)
                .profileAndSave(dataset, version("profile-stage"));
    }

    private TrainingContext train(String jobId,
                                  RahaDataset dataset,
                                  SampledLabels sampledLabels,
                                  InMemoryRahaRepository storage,
                                  InMemoryFmdbTableGateway fmdbGateway) {
        StrategyRepository strategyRepository = new DefaultStrategyRepository(storage);
        ModelMetadataRepository metadataRepository =
                new DefaultModelMetadataRepository(storage);
        FmdbModelStore modelStore = new FmdbModelStore(spark, fmdbGateway,
                MODEL_TABLE, DICTIONARY_TABLE, clock);
        ModelReleaseManager releaseManager = new ModelReleaseManager(
                metadataRepository, clock);
        RahaTrainService trainService = new RahaTrainService(
                new StrategyPlanService(new StrategyPlanGenerator(),
                        strategyRepository, clock),
                new StrategyExecutionService(new StrategyExecutor(
                        StrategyRegistry.defaults(), clock), strategyRepository, clock),
                new FeatureService(new FeatureAssembler(
                        new FeatureDictionaryVersioner(), clock),
                        new DefaultFeatureRepository(storage), clock),
                new ColumnClusteringService(new ScalableColumnClusterer(
                        new ClusterVersioner(), clock),
                        new DefaultClusterRepository(storage), clock),
                new com.fiberhome.ml.raha.label.LabelPropagationService(
                        new DefaultCellLabelRepository(storage), clock),
                new ColumnTrainingDataBuilder(), new AdaptiveColumnModelTrainer(
                        new SparkMllibLogisticRegressionTrainer(
                                spark, new ColumnModelVersioner()),
                        new WeightedRuleFallbackTrainer(new ColumnModelVersioner())),
                modelStore, new ColumnModelMetadataFactory(clock),
                releaseManager, clock);
        RahaJobConfig jobConfig = configFactory.jobConfig(JobType.TRAINING,
                DATASET_ID, DIRTY_TABLE, ROW_ID_COLUMN);
        LOGGER.info("开始执行训练服务，jobId={}，labelCount={}",
                jobId, sampledLabels.labels.size());
        RahaTaskResult<RahaTrainOutput> trained = trainService.train(
                new RahaTrainRequest(jobId, "train-operation", dataset,
                        jobConfig, sampledLabels.labels,
                        LabelPropagationMethod.HOMOGENEITY,
                        configFactory.labelPropagationConfig(),
                        configFactory.logisticRegressionTrainingConfig(),
                        DATASET_ID, version("train-stage"),
                        sampledLabels.preparation));
        if (trained.getStatus() == RahaTaskStatus.FAILED
                || trained.getPayload() == null
                || trained.getPayload().getCandidateModels().isEmpty()) {
            throw new IllegalStateException("训练服务未生成可发布候选模型："
                    + trained.getErrorMessage());
        }
        for (FeatureDictionary dictionary
                : trained.getPayload().getFeatures().getDictionaries().values()) {
            modelStore.saveDictionary(dictionary);
        }
        Map<String, RahaColumnModel> tunedModels = tuneCandidateThresholds(
                trained, modelStore, metadataRepository,
                sampledLabels.evaluationSplit);
        for (Map.Entry<String, RahaColumnModel> entry : tunedModels.entrySet()) {
            releaseManager.publish(DATASET_ID, entry.getKey(),
                    entry.getValue().getModelVersion(), version("publish-stage"));
        }
        LOGGER.info("训练服务执行完成，jobId={}，candidateModelCount={}",
                jobId, trained.getPayload().getCandidateModels().size());
        return new TrainingContext(trained, metadataRepository, tunedModels);
    }

    private Map<String, RahaColumnModel> tuneCandidateThresholds(
            RahaTaskResult<RahaTrainOutput> trained,
            FmdbModelStore modelStore,
            ModelMetadataRepository metadataRepository,
            EvaluationSplit evaluationSplit) {
        Map<String, CellLabel> validationTruth =
                new LinkedHashMap<String, CellLabel>();
        for (CellLabel label : evaluationSplit.getValidationLabels()) {
            validationTruth.put(label.getCellId(), label);
        }
        Map<String, RahaColumnModel> tuned =
                new LinkedHashMap<String, RahaColumnModel>();
        ThresholdComparisonService thresholdService = new ThresholdComparisonService(
                new DetectionEvaluationService(), metadataRepository, clock);
        for (Map.Entry<String, RahaColumnModel> entry
                : trained.getPayload().getCandidateModels().entrySet()) {
            RahaColumnModel candidate = entry.getValue();
            if (!"act_dep_time".equalsIgnoreCase(entry.getKey())) {
                tuned.put(entry.getKey(), candidate);
                continue;
            }
            ColumnModelArtifact artifact = modelStore.load(candidate.getModelPath());
            List<CellScore> scores = new ArrayList<CellScore>();
            List<CellLabel> labels = new ArrayList<CellLabel>();
            for (SparseFeatureRow row : trained.getPayload().getFeatures()
                    .getRowsByColumn(entry.getKey())) {
                CellLabel label = validationTruth.get(row.getCellId());
                if (label != null) {
                    scores.add(new CellScore(row.getCellId(), artifact.score(row)));
                    labels.add(label);
                }
            }
            if (!containsBothClasses(labels)) {
                LOGGER.warn("字段阈值验证集缺少正负样本，保留默认阈值，columnName={}，"
                                + "validationLabelCount={}",
                        entry.getKey(), labels.size());
                tuned.put(entry.getKey(), candidate);
                continue;
            }
            ThresholdComparisonResult comparison = thresholdService.compareAndSave(
                    candidate, scores, labels, thresholdCandidates(),
                    new ThresholdSelectionPolicy(0.72d, 0.03d, 0.5d),
                    version("threshold-stage"));
            tuned.put(entry.getKey(), comparison.getUpdatedModel());
        }
        return tuned;
    }

    private static boolean containsBothClasses(List<CellLabel> labels) {
        boolean positive = false;
        boolean negative = false;
        for (CellLabel label : labels) {
            positive |= label.getLabel() == 1;
            negative |= label.getLabel() == 0;
        }
        return positive && negative;
    }

    private static List<Double> thresholdCandidates() {
        List<Double> thresholds = new ArrayList<Double>();
        for (int value = 1; value < 100; value++) {
            thresholds.add(value / 100.0d);
        }
        return thresholds;
    }

    private RahaTaskResult<RahaDetectOutput> detect(
            String jobId,
            String modelVersion,
            String requestConfigVersion,
            RahaDataset dataset,
            TrainingContext training,
            InMemoryRahaRepository storage,
            InMemoryFmdbTableGateway fmdbGateway) {
        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(storage);
        FmdbModelStore restartedStore = new FmdbModelStore(spark, fmdbGateway,
                MODEL_TABLE, DICTIONARY_TABLE, clock);
        RahaDetectService detectService = new RahaDetectService(
                new PublishedColumnModelLoader(training.metadataRepository,
                        restartedStore, new ColumnModelCompatibilityValidator()),
                new ColumnModelPredictor(), detectionRepository, clock);
        LOGGER.info("开始执行检测服务，jobId={}，modelVersion={}",
                jobId, modelVersion);
        RahaTaskResult<RahaDetectOutput> detected = detectService.detect(
                new RahaDetectRequest(jobId, "detect-operation", requestConfigVersion,
                        modelVersion, dataset,
                        training.trained.getPayload().getFeatures(),
                        training.trained.getPayload().getStrategyPlanVersion(),
                        version("detect-operation"), configFactory.resourceConfig()));
        if (detected.getStatus() == RahaTaskStatus.FAILED
                || detected.getPayload() == null
                || detected.getPayload().getResults().isEmpty()) {
            throw new IllegalStateException("检测服务执行失败："
                    + detected.getErrorMessage());
        }
        // 单一类别字段不会生成模型，其他字段仍可按部分成功结果继续评估。
        if (detected.getStatus() != RahaTaskStatus.SUCCEEDED) {
            LOGGER.warn("检测服务部分成功，jobId={}，errorCode={}，errorMessage={}",
                    jobId, detected.getErrorCode(), detected.getErrorMessage());
        }
        LOGGER.info("检测服务执行完成，jobId={}，resultCount={}",
                jobId, detected.getPayload().getResults().size());
        return detected;
    }

    private long writeDetectionResults(RahaTaskResult<RahaDetectOutput> detected,
                                       InMemoryFmdbTableGateway fmdbGateway) {
        SparkSqlFmdbResultWriter writer = new SparkSqlFmdbResultWriter(
                spark, fmdbGateway, clock);
        long resultCount = writer.writeDetectionResults(RESULT_TABLE,
                DETECT_JOB_ID, detected.getPayload().getResults());
        LOGGER.info("开始写出脱敏检测结果，tableName={}，outputDirectory={}",
                RESULT_TABLE, outputDirectory);
        fmdbGateway.read(RESULT_TABLE).coalesce(1).write().mode("overwrite")
                .json(outputDirectory.resolve("detection-results").toUri().toString());
        return resultCount;
    }

    private String callSampleUdf(F_DW_RAHASAMPLE udf, String encodedRequest) {
        LOGGER.info("开始直接调用采样 UDF");
        String result = udf.call(encodedRequest);
        LOGGER.info("采样 UDF 直接调用完成，result={}", result);
        return result;
    }

    private String callTrainUdf(F_DW_RAHATRAIN udf, String encodedRequest) {
        LOGGER.info("开始直接调用训练 UDF");
        String result = udf.call(encodedRequest);
        LOGGER.info("训练 UDF 直接调用完成，result={}", result);
        return result;
    }

    private String callDetectUdf(F_DW_RAHADETECT udf, String encodedRequest) {
        LOGGER.info("开始直接调用检测 UDF");
        String result = udf.call(encodedRequest);
        LOGGER.info("检测 UDF 直接调用完成，result={}", result);
        return result;
    }

    private String sampleUdfRequest() {
        Map<String, String> values = commonUdfValues(SAMPLE_JOB_ID);
        values.put("labelingBudget", String.valueOf(
                configFactory.samplingConfig().getLabelingBudget()));
        return FormDataCodec.encode(values);
    }

    private String trainUdfRequest() {
        Map<String, String> values = commonUdfValues(TRAIN_JOB_ID);
        values.put("annotationReference", CLEAN_TABLE);
        return FormDataCodec.encode(values);
    }

    private String detectUdfRequest(String modelVersion) {
        Map<String, String> values = commonUdfValues(DETECT_JOB_ID);
        values.put("modelVersion", modelVersion);
        return FormDataCodec.encode(values);
    }

    private Map<String, String> commonUdfValues(String jobId) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("datasetId", DATASET_ID);
        values.put("inputReference", DIRTY_TABLE);
        values.put("sourceType", "TABLE");
        values.put("rowIdColumn", ROW_ID_COLUMN);
        values.put("snapshotId", SNAPSHOT_ID);
        values.put("idempotencyKey", jobId);
        values.put("caller", "container-validation");
        values.put("resultTable", RESULT_TABLE);
        return values;
    }

    private RahaUdfExecutionResult executeSampleUdfTask(
            RahaSampleUdfRequest request,
            RahaDataset dirtyDataset,
            GroundTruthDifferenceResult truth,
            InMemoryRahaRepository coreStorage,
            AtomicReference<SampledLabels> sampledReference) {
        long executionStartedAt = Math.max(1L, clock.millis());
        validateDataset(request.getDatasetId(), dirtyDataset);
        SampledLabels sampled = sampleLabels(request.getIdempotencyKey(),
                request.getLabelingBudget(), dirtyDataset, truth, coreStorage);
        sampledReference.set(sampled);
        Map<String, String> details = new LinkedHashMap<String, String>();
        details.put("sampledRowCount", String.valueOf(sampled.rowIds.size()));
        details.put("directLabelCount", String.valueOf(sampled.labels.size()));
        RahaTaskSummary summary = new RahaTaskSummary(executionStartedAt,
                Math.max(executionStartedAt, clock.millis()), sampled.rowIds.size(),
                sampled.rowIds.size(), 0L, 0L, details);
        return RahaUdfExecutionResult.completed(request.getIdempotencyKey(),
                RahaTaskType.SAMPLE,
                "repository://annotation-task/" + request.getIdempotencyKey(),
                HashUtils.sha256Hex(request.toCanonicalConfiguration()), summary);
    }

    private RahaUdfExecutionResult executeTrainUdfTask(
            RahaTrainUdfRequest request,
            RahaDataset dirtyDataset,
            InMemoryRahaRepository coreStorage,
            InMemoryFmdbTableGateway fmdbGateway,
            AtomicReference<SampledLabels> sampledReference,
            AtomicReference<TrainingContext> trainingReference) {
        validateDataset(request.getDatasetId(), dirtyDataset);
        if (!CLEAN_TABLE.equals(request.getAnnotationReference())) {
            throw new IllegalArgumentException("验收训练请求标注表不一致");
        }
        SampledLabels sampled = requireValue("采样", sampledReference.get());
        TrainingContext training = train(request.getIdempotencyKey(),
                dirtyDataset, sampled, coreStorage, fmdbGateway);
        trainingReference.set(training);
        return RahaUdfExecutionResult.fromTaskResult(training.trained,
                HashUtils.sha256Hex(request.toCanonicalConfiguration()));
    }

    private RahaUdfExecutionResult executeDetectUdfTask(
            RahaDetectUdfRequest request,
            RahaDataset dirtyDataset,
            InMemoryRahaRepository coreStorage,
            InMemoryFmdbTableGateway fmdbGateway,
            AtomicReference<TrainingContext> trainingReference,
            AtomicReference<RahaTaskResult<RahaDetectOutput>> detectedReference) {
        validateDataset(request.getDatasetId(), dirtyDataset);
        TrainingContext training = requireValue("训练", trainingReference.get());
        String requestConfigVersion = HashUtils.sha256Hex(
                request.toCanonicalConfiguration());
        RahaTaskResult<RahaDetectOutput> detected = detect(
                request.getIdempotencyKey(), request.getModelVersion(),
                requestConfigVersion, dirtyDataset, training, coreStorage, fmdbGateway);
        detectedReference.set(detected);
        return RahaUdfExecutionResult.fromTaskResult(detected,
                requestConfigVersion);
    }

    private static void validateDataset(String requestedDatasetId,
                                        RahaDataset dataset) {
        if (!dataset.getDatasetId().equals(requestedDatasetId)) {
            throw new IllegalArgumentException("UDF 请求数据集与执行数据集不一致");
        }
    }

    private void writeSummary(long rowCount,
                              GroundTruthDifferenceResult truth,
                              TrainingContext training,
                              RahaTaskResult<RahaDetectOutput> detected,
                              long resultCount,
                              DetectionEvaluationMetrics metrics,
                              DetectionEvaluationMetrics fullMetrics,
                              SampledLabels sampledLabels,
                              String trainUdfResult,
                              String detectUdfResult,
                              String sampleUdfResult,
                              long elapsedMillis) {
        String summary = "{\n"
                + "  \"datasetId\": \"" + escape(DATASET_ID) + "\",\n"
                + "  \"rowIdColumn\": \"" + escape(ROW_ID_COLUMN) + "\",\n"
                + "  \"sparkVersion\": \"" + escape(spark.version()) + "\",\n"
                + "  \"sparkMaster\": \"" + escape(spark.sparkContext().master()) + "\",\n"
                + "  \"rowCount\": " + rowCount + ",\n"
                + "  \"detectableCellCount\": " + truth.getLabels().size() + ",\n"
                + "  \"truthPositiveCount\": " + truth.getPositiveCount() + ",\n"
                + "  \"truthNegativeCount\": " + truth.getNegativeCount() + ",\n"
                + "  \"sampledRowCount\": " + sampledLabels.rowIds.size() + ",\n"
                + "  \"directLabelCount\": " + sampledLabels.labels.size() + ",\n"
                + "  \"samplingMode\": \"ACTIVE_CLUSTER_COVERAGE\",\n"
                + "  \"sampledRowIds\": \""
                + escape(sampledLabels.samplingOrder.toString()) + "\",\n"
                + "  \"featurePreparationMillis\": "
                + sampledLabels.preparation.getRuntimeMillis() + ",\n"
                + "  \"thresholdValidationCellCount\": "
                + sampledLabels.evaluationSplit.getValidationLabels().size() + ",\n"
                + "  \"finalTestCellCount\": "
                + sampledLabels.evaluationSplit.getTestLabels().size() + ",\n"
                + "  \"evaluationCellCount\": " + metrics.getEvaluatedCellCount() + ",\n"
                + "  \"strategyPlanCount\": "
                + training.trained.getSummary().getDetails().get("strategyPlanCount") + ",\n"
                + "  \"strategyHitCount\": "
                + training.trained.getSummary().getDetails().get("strategyHitCount") + ",\n"
                + "  \"strategyFamilyPlanCounts\": \""
                + escape(training.trained.getSummary().getDetails()
                        .get("strategyFamilyPlanCounts")) + "\",\n"
                + "  \"candidateModelCount\": "
                + training.trained.getPayload().getCandidateModels().size() + ",\n"
                + "  \"detectionResultCount\": "
                + detected.getPayload().getResults().size() + ",\n"
                + "  \"persistedResultCount\": " + resultCount + ",\n"
                + "  \"truePositive\": " + metrics.getTruePositive() + ",\n"
                + "  \"falsePositive\": " + metrics.getFalsePositive() + ",\n"
                + "  \"falseNegative\": " + metrics.getFalseNegative() + ",\n"
                + "  \"trueNegative\": " + metrics.getTrueNegative() + ",\n"
                + "  \"precision\": " + decimal(metrics.getPrecision()) + ",\n"
                + "  \"recall\": " + decimal(metrics.getRecall()) + ",\n"
                + "  \"f1\": " + decimal(metrics.getF1()) + ",\n"
                + "  \"averagePrecision\": "
                + decimal(metrics.getAveragePrecision()) + ",\n"
                + "  \"fullDataPrecision\": " + decimal(fullMetrics.getPrecision()) + ",\n"
                + "  \"fullDataRecall\": " + decimal(fullMetrics.getRecall()) + ",\n"
                + "  \"fullDataF1\": " + decimal(fullMetrics.getF1()) + ",\n"
                + "  \"scoreDiagnostics\": \""
                + escape(detected.getSummary().getDetails().get("scoreDiagnostics"))
                + "\",\n"
                + "  \"modelDiagnostics\": \"" + escape(modelDiagnostics(training))
                + "\",\n"
                + "  \"selectedThresholds\": \""
                + escape(selectedThresholds(training)) + "\",\n"
                + "  \"elapsedMillis\": " + elapsedMillis + ",\n"
                + "  \"sampleUdfResult\": \"" + escape(sampleUdfResult) + "\",\n"
                + "  \"trainUdfResult\": \"" + escape(trainUdfResult) + "\",\n"
                + "  \"detectUdfResult\": \"" + escape(detectUdfResult) + "\"\n"
                + "}\n";
        Path summaryPath = outputDirectory.resolve("validation-summary.json");
        try {
            LOGGER.info("开始写入验收摘要，summaryPath={}", summaryPath);
            Files.write(summaryPath, summary.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (java.io.IOException exception) {
            LOGGER.error("写入验收摘要失败，summaryPath={}", summaryPath, exception);
            throw new IllegalStateException("无法写入验收摘要", exception);
        }
        System.out.println("RAHA_VALIDATION_SUMMARY=" + summaryPath);
        System.out.println("RAHA_VALIDATION_PRECISION=" + decimal(metrics.getPrecision()));
        System.out.println("RAHA_VALIDATION_RECALL=" + decimal(metrics.getRecall()));
        System.out.println("RAHA_VALIDATION_F1=" + decimal(metrics.getF1()));
    }

    private ArtifactVersion version(String stageId) {
        return new ArtifactVersion(configVersion, SNAPSHOT_ID, stageId, 1);
    }

    private static void requireCompleted(String taskName, String result) {
        if (result == null || (!result.contains("\"status\":\"SUCCEEDED\"")
                && !result.contains("\"status\":\"PARTIAL_SUCCESS\""))) {
            throw new IllegalStateException(taskName + " UDF 未完成业务执行：" + result);
        }
    }

    private static <T> T requireValue(String taskName, T value) {
        if (value == null) {
            throw new IllegalStateException(taskName + " UDF 没有生成结果");
        }
        return value;
    }

    private static String decimal(double value) {
        return String.format(Locale.ROOT, "%.12f", value);
    }

    private static String modelDiagnostics(TrainingContext training) {
        Map<String, Map<String, Double>> metrics =
                new LinkedHashMap<String, Map<String, Double>>();
        for (Map.Entry<String, RahaColumnModel> entry
                : training.tunedModels.entrySet()) {
            metrics.put(entry.getKey(), entry.getValue().getMetrics());
        }
        return metrics.toString();
    }

    private static String selectedThresholds(TrainingContext training) {
        Map<String, Double> thresholds = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, RahaColumnModel> entry
                : training.tunedModels.entrySet()) {
            thresholds.put(entry.getKey(), entry.getValue().getThreshold());
        }
        return thresholds.toString();
    }

    private static String sparkPath(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Spark 数据路径不能为空");
        }
        if (value.matches("^[A-Za-z][A-Za-z0-9+.-]*:.*")) {
            return value;
        }
        return Paths.get(value).toAbsolutePath().normalize().toUri().toString();
    }

    private SampledLabels sampleLabels(String jobId,
                                       int budget,
                                       RahaDataset dataset,
                                       GroundTruthDifferenceResult truth,
                                       InMemoryRahaRepository storage) {
        RahaJobConfig jobConfig = configFactory.jobConfig(JobType.TRAINING,
                DATASET_ID, DIRTY_TABLE, ROW_ID_COLUMN);
        long randomSeed = jobConfig.getRandomSeed();
        StrategyRepository strategyRepository = new DefaultStrategyRepository(storage);
        RahaFeaturePreparationService preparationService =
                new RahaFeaturePreparationService(
                        new StrategyPlanService(new StrategyPlanGenerator(),
                                strategyRepository, clock),
                        new StrategyExecutionService(new StrategyExecutor(
                                StrategyRegistry.defaults(), clock),
                                strategyRepository, clock),
                        new FeatureService(new FeatureAssembler(
                                new FeatureDictionaryVersioner(), clock),
                                new DefaultFeatureRepository(storage), clock),
                        clock);
        RahaFeaturePreparationResult preparation = preparationService.prepare(
                new RahaFeaturePreparationRequest(jobId,
                        "sample-prepare", dataset, jobConfig,
                        version("sample-prepare")));
        int availableRowCount = featureRowCount(preparation.getFeatures());
        int samplingBudget = Math.min(budget, Math.max(1, availableRowCount - 1));
        if (samplingBudget < budget) {
            LOGGER.warn("验收数据行数不足以同时满足采样和评测，自动保留一行，"
                            + "requestedBudget={}，samplingBudget={}，rowCount={}",
                    budget, samplingBudget, availableRowCount);
        }
        ColumnClusteringService clusteringService = new ColumnClusteringService(
                new ScalableColumnClusterer(new ClusterVersioner(), clock),
                new DefaultClusterRepository(storage), clock);
        SamplingService samplingService = new SamplingService(
                new ClusterCoverageScorer(), new TupleSampler(),
                new SamplingVersioner(),
                new DefaultAnnotationTaskRepository(storage), clock);
        RahaSampleService sampleService = new RahaSampleService(
                clusteringService, samplingService, clock);
        ClusteringConfig baseClustering = configFactory.clusteringConfig();
        ClusteringConfig activeClustering = new ClusteringConfig(
                baseClustering.getDistanceMetric(),
                Math.max(baseClustering.getTargetClusterCount(), samplingBudget + 2),
                baseClustering.getMaxSampleCount());
        Map<String, CellLabel> truthByCell = new LinkedHashMap<String, CellLabel>();
        for (CellLabel label : truth.getLabels()) {
            truthByCell.put(label.getCellId(), label);
        }
        final long[] createdAt = new long[]{Math.max(1L, clock.millis())};
        ActiveSamplingResult activeResult = new ActiveSamplingOrchestrator(sampleService)
                .sample(jobId, preparation.getFeatures(),
                        Collections.<CellLabel>emptyList(), activeClustering,
                        configFactory.samplingConfig(), samplingBudget, randomSeed,
                        version("sample-active"), configFactory.resourceConfig(),
                        new SampledTupleLabelProvider() {
                            @Override
                            public List<CellLabel> labelsFor(AnnotationTask task) {
                                List<CellLabel> labels = new ArrayList<CellLabel>();
                                for (ColumnMetadata column : dataset.getColumns()) {
                                    if (ROW_ID_COLUMN.equals(column.getName())) {
                                        continue;
                                    }
                                    String cellId = new CellCoordinate(DATASET_ID,
                                            SNAPSHOT_ID, task.getRowId(),
                                            column.getName()).toCellId();
                                    CellLabel truthLabel = truthByCell.get(cellId);
                                    if (truthLabel == null) {
                                        throw new IllegalStateException(
                                                "采样单元格缺少真值标签");
                                    }
                                    labels.add(new CellLabel(cellId,
                                            truthLabel.getLabel(), LabelSource.HUMAN,
                                            1.0d, null, null,
                                            "container-validation", createdAt[0]++));
                                }
                                return labels;
                            }
                        });
        LOGGER.info("逐轮聚类主动采样完成，rowCount={}，directLabelCount={}，"
                        + "randomSeed={}，preparationMillis={}",
                activeResult.getRowIds().size(), activeResult.getLabels().size(),
                randomSeed, preparation.getRuntimeMillis());
        // 对齐文件写出后，释放策略命中对象及内存仓储副本，避免训练阶段驱动端堆积。
        new StrategyAlignmentArtifactWriter().write(
                outputDirectory.resolve("java-strategy-alignment.jsonl"),
                preparation.getStrategyPlans(), preparation.getStrategyBatch());
        int removedHitRecords = storage.removePartition(RepositoryNamespace.STRATEGY_HIT,
                jobId);
        LOGGER.info("策略命中中间结果已释放，jobId={}，removedRecordCount={}",
                jobId, removedHitRecords);
        preparation = preparation.withClustering(activeResult.getClustering())
                .withoutStrategyHits();
        EvaluationSplit evaluationSplit = new EvaluationSplitService().split(
                truth.getLabels(), activeResult.getCellIds(), 5, 0,
                DATASET_ID + "|" + SNAPSHOT_ID + "|threshold-v1");
        return new SampledLabels(activeResult.getLabels(),
                new LinkedHashSet<String>(activeResult.getRowIds()),
                activeResult.getCellIds(), preparation,
                activeResult.getRowIds(), evaluationSplit);
    }

    /**
     * 从稳定特征坐标统计可参与主动采样的元组数量。
     *
     * @param features 已准备单元格特征
     * @return 去重后的元组数量
     */
    private static int featureRowCount(FeatureAssemblyResult features) {
        Set<String> rowIds = new LinkedHashSet<String>();
        for (SparseFeatureRow row : features.getRows()) {
            if (row != null && row.getCoordinate() != null) {
                rowIds.add(row.getCoordinate().getRowId());
            }
        }
        if (rowIds.isEmpty()) {
            throw new IllegalStateException("主动采样特征缺少稳定元组坐标");
        }
        return rowIds.size();
    }

    private static EvaluationSlice evaluationSlice(
            List<DetectionResult> detections,
            List<CellLabel> labels) {
        Set<String> includedCellIds = new HashSet<String>();
        for (CellLabel label : labels) {
            includedCellIds.add(label.getCellId());
        }
        List<DetectionResult> holdoutDetections = new ArrayList<DetectionResult>();
        for (DetectionResult detection : detections) {
            if (includedCellIds.contains(detection.getCoordinate().toCellId())) {
                holdoutDetections.add(detection);
            }
        }
        return new EvaluationSlice(holdoutDetections, labels);
    }

    private static String tableToken(String datasetId) {
        String token = datasetId == null ? "dataset"
                : datasetId.replaceAll("[^A-Za-z0-9_]", "_");
        if (token.isEmpty()) {
            token = "dataset";
        }
        if (Character.isDigit(token.charAt(0))) {
            token = "d_" + token;
        }
        return token.length() <= 40 ? token : token.substring(0, 40);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    /**
     * 保存训练产物及其跨进程重新加载所需仓储。
     */
    private static final class TrainingContext {
        /** 训练服务结果。 */
        private final RahaTaskResult<RahaTrainOutput> trained;
        /** 已发布模型元数据仓储。 */
        private final ModelMetadataRepository metadataRepository;
        /** 完成独立验证阈值选择后的候选模型。 */
        private final Map<String, RahaColumnModel> tunedModels;

        private TrainingContext(RahaTaskResult<RahaTrainOutput> trained,
                                ModelMetadataRepository metadataRepository,
                                Map<String, RahaColumnModel> tunedModels) {
            this.trained = trained;
            this.metadataRepository = metadataRepository;
            this.tunedModels = Collections.unmodifiableMap(
                    new LinkedHashMap<String, RahaColumnModel>(tunedModels));
        }
    }

    /** 保存固定预算直接标签及其训练排除坐标。 */
    private static final class SampledLabels {
        /** 传入训练服务的人工标签。 */
        private final List<CellLabel> labels;
        /** 已采样稳定行标识。 */
        private final Set<String> rowIds;
        /** 最终评估需要排除的直接标注单元格。 */
        private final Set<String> cellIds;
        /** SAMPLE 阶段生成并由 TRAIN 复用的策略及特征产物。 */
        private final RahaFeaturePreparationResult preparation;
        /** 按主动采样轮次保存的行标识。 */
        private final List<String> samplingOrder;
        /** 独立阈值验证集和最终测试集。 */
        private final EvaluationSplit evaluationSplit;

        private SampledLabels(List<CellLabel> labels,
                              Set<String> rowIds,
                              Set<String> cellIds,
                              RahaFeaturePreparationResult preparation,
                              List<String> samplingOrder,
                              EvaluationSplit evaluationSplit) {
            this.labels = Collections.unmodifiableList(
                    new ArrayList<CellLabel>(labels));
            this.rowIds = Collections.unmodifiableSet(new HashSet<String>(rowIds));
            this.cellIds = Collections.unmodifiableSet(new HashSet<String>(cellIds));
            this.preparation = preparation;
            this.samplingOrder = Collections.unmodifiableList(
                    new ArrayList<String>(samplingOrder));
            if (preparation == null || evaluationSplit == null) {
                throw new IllegalArgumentException("采样特征产物和评测划分不能为空");
            }
            this.evaluationSplit = evaluationSplit;
        }
    }

    /** 保存排除直接标注坐标后的检测结果和真值。 */
    private static final class EvaluationSlice {
        /** 保留集检测结果。 */
        private final List<DetectionResult> detections;
        /** 保留集真值。 */
        private final List<CellLabel> labels;

        private EvaluationSlice(List<DetectionResult> detections,
                                List<CellLabel> labels) {
            this.detections = detections;
            this.labels = labels;
        }
    }

}
