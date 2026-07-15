package com.fiberhome.ml.raha.app;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.HierarchicalColumnClusterer;
import com.fiberhome.ml.raha.config.RahaConfigFactory;
import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.RahaJobConfig;
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
import com.fiberhome.ml.raha.evaluation.GroundTruthDifferenceResult;
import com.fiberhome.ml.raha.evaluation.GroundTruthDifferenceService;
import com.fiberhome.ml.raha.feature.FeatureAssembler;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.fmdb.DefaultFmdbSchemaResolver;
import com.fiberhome.ml.raha.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.fmdb.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.fmdb.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.label.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.AdaptiveColumnModelTrainer;
import com.fiberhome.ml.raha.model.ColumnModelCompatibilityValidator;
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
import com.fiberhome.ml.raha.repository.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.StrategyRepository;
import com.fiberhome.ml.raha.service.RahaDetectOutput;
import com.fiberhome.ml.raha.service.RahaDetectRequest;
import com.fiberhome.ml.raha.service.RahaDetectService;
import com.fiberhome.ml.raha.service.RahaTaskResult;
import com.fiberhome.ml.raha.service.RahaTaskStatus;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.service.RahaTrainOutput;
import com.fiberhome.ml.raha.service.RahaTrainRequest;
import com.fiberhome.ml.raha.service.RahaTrainService;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.sampling.TupleSamplingScore;
import com.fiberhome.ml.raha.strategy.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.StrategyPlanService;
import com.fiberhome.ml.raha.strategy.StrategyRegistry;
import com.fiberhome.ml.raha.udf.FileRahaUdfJobWorker;
import com.fiberhome.ml.raha.udf.RahaUdfJobSubmitter;
import com.fiberhome.ml.raha.udf.RahaUdfRegistrar;
import com.fiberhome.ml.raha.udf.RahaUdfRequest;
import com.fiberhome.ml.raha.udf.RahaUdfSubmissionResult;
import com.fiberhome.ml.raha.util.FormDataCodec;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 在 Spark 集群中使用样例脏表和真值表完成 UDF 建单、训练、检测与评估验收。
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
     * 串联 UDF 建单和生产服务，输出可复核的检测结果与评估摘要。
     */
    private void run() {
        long startedAt = System.currentTimeMillis();
        LOGGER.info("开始 Raha 容器验收，dirtyPath={}，cleanPath={}，outputDirectory={}",
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

            Path queueDirectory = outputDirectory.resolve("udf-requests");
            RahaUdfRegistrar registrar = new RahaUdfRegistrar();
            registrar.register(spark, new SharedFileRahaUdfJobSubmitter(
                    queueDirectory.toString()));
            AtomicReference<SampledLabels> sampledReference =
                    new AtomicReference<SampledLabels>();
            AtomicReference<TrainingContext> trainingReference =
                    new AtomicReference<TrainingContext>();
            AtomicReference<RahaTaskResult<RahaDetectOutput>> detectedReference =
                    new AtomicReference<RahaTaskResult<RahaDetectOutput>>();
            FileRahaUdfJobWorker worker = new FileRahaUdfJobWorker(queueDirectory,
                    request -> dispatchUdfTask(request, dirtyDataset, truth,
                            coreStorage, fmdbGateway, sampledReference,
                            trainingReference, detectedReference), clock);

            String sampleUdfResult = callUdf(RahaUdfRegistrar.SAMPLE_FUNCTION,
                    udfRequest(RahaTaskType.SAMPLE, null));
            requireAccepted("采样", sampleUdfResult);
            requireProcessed("采样", worker.runOnce());
            SampledLabels sampledLabels = requireValue("采样", sampledReference.get());

            String trainUdfResult = callUdf(RahaUdfRegistrar.TRAIN_FUNCTION,
                    udfRequest(RahaTaskType.TRAIN, null));
            requireAccepted("训练", trainUdfResult);
            requireProcessed("训练", worker.runOnce());
            TrainingContext training = requireValue("训练", trainingReference.get());
            String modelVersion = firstModelVersion(training.trained);
            String detectUdfResult = callUdf(RahaUdfRegistrar.DETECT_FUNCTION,
                    udfRequest(RahaTaskType.DETECT, modelVersion));
            requireAccepted("检测", detectUdfResult);
            requireProcessed("检测", worker.runOnce());
            RahaTaskResult<RahaDetectOutput> detected = requireValue(
                    "检测", detectedReference.get());
            DetectionEvaluationMetrics fullMetrics = new DetectionEvaluationService()
                    .evaluate(detected.getPayload().getResults(), truth.getLabels());
            EvaluationSlice holdout = holdout(detected.getPayload().getResults(),
                    truth.getLabels(), sampledLabels.cellIds);
            DetectionEvaluationMetrics metrics = new DetectionEvaluationService()
                    .evaluate(holdout.detections, holdout.labels);
            long resultCount = writeDetectionResults(detected, fmdbGateway);
            writeSummary(dirtyRowCount, truth, training, detected, resultCount,
                    metrics, fullMetrics, sampledLabels, trainUdfResult, detectUdfResult,
                    sampleUdfResult,
                    System.currentTimeMillis() - startedAt);
            LOGGER.info("Raha 容器验收完成，rowCount={}，truthPositiveCount={}，"
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
            Files.createDirectories(outputDirectory.resolve("udf-requests"));
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

    private TrainingContext train(RahaDataset dataset,
                                  List<CellLabel> directLabels,
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
                new ColumnClusteringService(new HierarchicalColumnClusterer(
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
                TRAIN_JOB_ID, directLabels.size());
        RahaTaskResult<RahaTrainOutput> trained = trainService.train(
                new RahaTrainRequest(TRAIN_JOB_ID, "train-stage", dataset,
                        jobConfig, directLabels, LabelPropagationMethod.HOMOGENEITY,
                        configFactory.labelPropagationConfig(),
                        configFactory.logisticRegressionTrainingConfig(),
                        DATASET_ID, version("train-stage")));
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
        for (Map.Entry<String, RahaColumnModel> entry
                : trained.getPayload().getCandidateModels().entrySet()) {
            releaseManager.publish(DATASET_ID, entry.getKey(),
                    entry.getValue().getModelVersion(), version("publish-stage"));
        }
        LOGGER.info("训练服务执行完成，jobId={}，candidateModelCount={}",
                TRAIN_JOB_ID, trained.getPayload().getCandidateModels().size());
        return new TrainingContext(trained, metadataRepository, modelStore);
    }

    private RahaTaskResult<RahaDetectOutput> detect(
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
        LOGGER.info("开始执行检测服务，jobId={}", DETECT_JOB_ID);
        RahaTaskResult<RahaDetectOutput> detected = detectService.detect(
                new RahaDetectRequest(DETECT_JOB_ID, "detect-stage", configVersion,
                        dataset, training.trained.getPayload().getFeatures(),
                        training.trained.getPayload().getStrategyPlanVersion(),
                        version("detect-stage"), configFactory.resourceConfig()));
        if (detected.getStatus() == RahaTaskStatus.FAILED
                || detected.getPayload() == null
                || detected.getPayload().getResults().isEmpty()) {
            throw new IllegalStateException("检测服务执行失败："
                    + detected.getErrorMessage());
        }
        // 单一类别字段不会生成模型，其他字段仍可按部分成功结果继续评估。
        if (detected.getStatus() != RahaTaskStatus.SUCCEEDED) {
            LOGGER.warn("检测服务部分成功，jobId={}，errorCode={}，errorMessage={}",
                    DETECT_JOB_ID, detected.getErrorCode(), detected.getErrorMessage());
        }
        LOGGER.info("检测服务执行完成，jobId={}，resultCount={}",
                DETECT_JOB_ID, detected.getPayload().getResults().size());
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

    private String callUdf(String functionName, String encodedRequest) {
        Dataset<Row> requests = spark.range(1L).repartition(1)
                .select(functions.lit(encodedRequest).alias("request"));
        LOGGER.info("开始通过 Spark SQL 调用 Raha UDF，functionName={}", functionName);
        String result = requests.selectExpr(functionName + "(request) AS result")
                .first().getString(0);
        LOGGER.info("Spark SQL Raha UDF 调用完成，functionName={}，result={}",
                functionName, result);
        return result;
    }

    private String udfRequest(RahaTaskType taskType, String modelVersion) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("datasetId", DATASET_ID);
        values.put("inputReference", DIRTY_TABLE);
        values.put("sourceType", "TABLE");
        values.put("rowIdColumn", ROW_ID_COLUMN);
        values.put("snapshotId", SNAPSHOT_ID);
        values.put("idempotencyKey", taskType == RahaTaskType.TRAIN
                ? TRAIN_JOB_ID : taskType == RahaTaskType.SAMPLE
                ? SAMPLE_JOB_ID : DETECT_JOB_ID);
        values.put("caller", "container-validation");
        values.put("resultTable", RESULT_TABLE);
        if (taskType == RahaTaskType.TRAIN) {
            values.put("annotationReference", CLEAN_TABLE);
        } else if (taskType == RahaTaskType.DETECT) {
            values.put("modelVersion", modelVersion);
        } else {
            values.put("labelingBudget", String.valueOf(
                    configFactory.samplingConfig().getLabelingBudget()));
        }
        return FormDataCodec.encode(values);
    }

    private String dispatchUdfTask(
            RahaUdfRequest request,
            RahaDataset dirtyDataset,
            GroundTruthDifferenceResult truth,
            InMemoryRahaRepository coreStorage,
            InMemoryFmdbTableGateway fmdbGateway,
            AtomicReference<SampledLabels> sampledReference,
            AtomicReference<TrainingContext> trainingReference,
            AtomicReference<RahaTaskResult<RahaDetectOutput>> detectedReference) {
        if (request.getTaskType() == RahaTaskType.SAMPLE) {
            SampledLabels sampled = sampleLabels(dirtyDataset, truth);
            sampledReference.set(sampled);
            return "sampledRowCount=" + sampled.rowIds.size();
        }
        if (request.getTaskType() == RahaTaskType.TRAIN) {
            SampledLabels sampled = requireValue("采样", sampledReference.get());
            TrainingContext training = train(dirtyDataset, sampled.labels,
                    coreStorage, fmdbGateway);
            trainingReference.set(training);
            return "candidateModelCount="
                    + training.trained.getPayload().getCandidateModels().size();
        }
        TrainingContext training = requireValue("训练", trainingReference.get());
        RahaTaskResult<RahaDetectOutput> detected = detect(dirtyDataset,
                training, coreStorage, fmdbGateway);
        detectedReference.set(detected);
        return "detectionResultCount=" + detected.getPayload().getResults().size();
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

    private static String firstModelVersion(RahaTaskResult<RahaTrainOutput> trained) {
        return trained.getPayload().getCandidateModels().values()
                .iterator().next().getModelVersion();
    }

    private static void requireAccepted(String taskName, String result) {
        if (result == null || (!result.contains("\"status\":\"ACCEPTED\"")
                && !result.contains("\"status\":\"DUPLICATE\""))) {
            throw new IllegalStateException(taskName + " UDF 未接受任务：" + result);
        }
    }

    private static void requireProcessed(String taskName, int processedCount) {
        if (processedCount != 1) {
            throw new IllegalStateException(taskName + " UDF 文件任务未被唯一处理");
        }
    }

    private static <T> T requireValue(String taskName, T value) {
        if (value == null) {
            throw new IllegalStateException(taskName + " UDF 文件任务没有生成结果");
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
                : training.trained.getPayload().getCandidateModels().entrySet()) {
            metrics.put(entry.getKey(), entry.getValue().getMetrics());
        }
        return metrics.toString();
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

    private SampledLabels sampleLabels(RahaDataset dataset,
                                       GroundTruthDifferenceResult truth) {
        int budget = configFactory.samplingConfig().getLabelingBudget();
        long randomSeed = configFactory.jobConfig(JobType.TRAINING,
                DATASET_ID, DIRTY_TABLE, ROW_ID_COLUMN).getRandomSeed();
        List<Row> rows = dataset.getDataFrame().select(ROW_ID_COLUMN).collectAsList();
        List<TupleSamplingScore> scores = new ArrayList<TupleSamplingScore>(rows.size());
        for (Row row : rows) {
            String rowId = row.get(0).toString();
            scores.add(new TupleSamplingScore(rowId, 1.0d, 1.0d,
                    Collections.singletonMap("validation", "uniform"),
                    Collections.<String, Double>emptyMap()));
        }
        List<TupleSamplingScore> selected = new TupleSampler().select(
                scores, budget, randomSeed);
        Set<String> selectedRows = new HashSet<String>();
        for (TupleSamplingScore score : selected) {
            selectedRows.add(score.getRowId());
        }
        Map<String, CellLabel> truthByCell = new HashMap<String, CellLabel>();
        for (CellLabel label : truth.getLabels()) {
            truthByCell.put(label.getCellId(), label);
        }
        List<CellLabel> directLabels = new ArrayList<CellLabel>();
        Set<String> directCellIds = new HashSet<String>();
        long createdAt = Math.max(1L, clock.millis());
        for (String rowId : selectedRows) {
            for (ColumnMetadata column : dataset.getColumns()) {
                if (ROW_ID_COLUMN.equals(column.getName())) {
                    continue;
                }
                String cellId = new CellCoordinate(DATASET_ID, SNAPSHOT_ID,
                        rowId, column.getName()).toCellId();
                CellLabel truthLabel = truthByCell.get(cellId);
                if (truthLabel == null) {
                    throw new IllegalStateException("采样单元格缺少真值标签");
                }
                directLabels.add(new CellLabel(cellId, truthLabel.getLabel(),
                        LabelSource.HUMAN, 1.0d, null, null,
                        "container-validation", createdAt++));
                directCellIds.add(cellId);
            }
        }
        LOGGER.info("固定预算采样完成，rowCount={}，directLabelCount={}，randomSeed={}",
                selectedRows.size(), directLabels.size(), randomSeed);
        return new SampledLabels(directLabels, selectedRows, directCellIds);
    }

    private static EvaluationSlice holdout(List<DetectionResult> detections,
                                           List<CellLabel> labels,
                                           Set<String> excludedCellIds) {
        List<DetectionResult> holdoutDetections = new ArrayList<DetectionResult>();
        for (DetectionResult detection : detections) {
            if (!excludedCellIds.contains(detection.getCoordinate().toCellId())) {
                holdoutDetections.add(detection);
            }
        }
        List<CellLabel> holdoutLabels = new ArrayList<CellLabel>();
        for (CellLabel label : labels) {
            if (!excludedCellIds.contains(label.getCellId())) {
                holdoutLabels.add(label);
            }
        }
        return new EvaluationSlice(holdoutDetections, holdoutLabels);
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
        /** 训练阶段模型存储，仅用于保留生命周期引用。 */
        private final FmdbModelStore modelStore;

        private TrainingContext(RahaTaskResult<RahaTrainOutput> trained,
                                ModelMetadataRepository metadataRepository,
                                FmdbModelStore modelStore) {
            this.trained = trained;
            this.metadataRepository = metadataRepository;
            this.modelStore = modelStore;
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

        private SampledLabels(List<CellLabel> labels,
                              Set<String> rowIds,
                              Set<String> cellIds) {
            this.labels = Collections.unmodifiableList(
                    new ArrayList<CellLabel>(labels));
            this.rowIds = Collections.unmodifiableSet(new HashSet<String>(rowIds));
            this.cellIds = Collections.unmodifiableSet(new HashSet<String>(cellIds));
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

    /**
     * 将执行器收到的 UDF 请求幂等写入共享目录，模拟生产异步任务队列边界。
     */
    private static final class SharedFileRahaUdfJobSubmitter
            implements RahaUdfJobSubmitter, Serializable {

        /** Java 序列化版本。 */
        private static final long serialVersionUID = 1L;
        /** 日志记录器。 */
        private static final Logger LOGGER = LoggerFactory.getLogger(
                SharedFileRahaUdfJobSubmitter.class);
        /** 所有 Spark 节点可读写的任务目录。 */
        private final String queueDirectory;

        private SharedFileRahaUdfJobSubmitter(String queueDirectory) {
            this.queueDirectory = queueDirectory;
        }

        @Override
        public RahaUdfSubmissionResult submit(RahaUdfRequest request) {
            long submittedAt = Math.max(1L, System.currentTimeMillis());
            String jobId = request.getIdempotencyKey();
            String configVersion = HashUtils.sha256Hex(
                    request.toCanonicalConfiguration());
            Path queuePath = Paths.get(queueDirectory,
                    jobId + "-" + request.getTaskType().name().toLowerCase(Locale.ROOT)
                            + ".request");
            Map<String, String> receipt = new LinkedHashMap<String, String>();
            receipt.put("jobId", jobId);
            receipt.put("taskType", request.getTaskType().name());
            receipt.put("datasetId", request.getDatasetId());
            receipt.put("inputReference", request.getInputReference());
            receipt.put("resultTable", request.getResultTable());
            receipt.put("configVersion", configVersion);
            SparkEnv environment = SparkEnv.get();
            receipt.put("executorId", environment == null
                    ? "unknown" : environment.executorId());
            receipt.put("submittedAt", String.valueOf(submittedAt));
            try {
                LOGGER.info("开始写入 Raha UDF 共享任务，jobId={}，taskType={}，queuePath={}",
                        jobId, request.getTaskType(), queuePath);
                Files.createDirectories(queuePath.getParent());
                Files.write(queuePath,
                        request.toEncodedRequest().getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                Files.write(queuePath.resolveSibling(
                                queuePath.getFileName().toString() + ".receipt"),
                        FormDataCodec.encode(receipt).getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                LOGGER.info("Raha UDF 共享任务写入完成，jobId={}，executorId={}",
                        jobId, receipt.get("executorId"));
                return RahaUdfSubmissionResult.accepted(jobId,
                        request.getTaskType(), queuePath.toUri().toString(),
                        configVersion, submittedAt);
            } catch (FileAlreadyExistsException exception) {
                LOGGER.info("Raha UDF 共享任务重复提交，jobId={}，queuePath={}",
                        jobId, queuePath);
                return RahaUdfSubmissionResult.duplicate(jobId,
                        request.getTaskType(), queuePath.toUri().toString(),
                        configVersion, submittedAt);
            } catch (java.io.IOException exception) {
                LOGGER.error("写入 Raha UDF 共享任务失败，jobId={}，queuePath={}",
                        jobId, queuePath, exception);
                throw new IllegalStateException("无法写入 Raha UDF 共享任务", exception);
            }
        }
    }
}
