package com.fiberhome.ml.raha.app;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.excel.AnnotationExcelConfig;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import com.fiberhome.ml.raha.annotation.service.AnnotationImportRequest;
import com.fiberhome.ml.raha.annotation.service.AnnotationImportResult;
import com.fiberhome.ml.raha.annotation.service.AnnotationImportService;
import com.fiberhome.ml.raha.annotation.service.AnnotationLabelExpander;
import com.fiberhome.ml.raha.annotation.service.AnnotationTemplateRequest;
import com.fiberhome.ml.raha.annotation.service.AnnotationTemplateService;
import com.fiberhome.ml.raha.config.core.RahaStorageMode;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.job.domain.RahaStage;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.prediction.ColumnPrediction;
import com.fiberhome.ml.raha.model.release.ModelReleaseManager;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.SparkSqlFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbAnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbSampleRecordRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbDetectionWriteContext;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.task.DetectionRequestOptions;
import com.fiberhome.ml.raha.service.task.FmdbInputSpec;
import com.fiberhome.ml.raha.service.task.MissingModelPolicy;
import com.fiberhome.ml.raha.service.task.RahaTaskApplicationService;
import com.fiberhome.ml.raha.service.task.RahaTaskApplicationServiceFactory;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionRequest;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionResult;
import com.fiberhome.ml.raha.service.task.RahaTaskRequestFactory;
import com.fiberhome.ml.raha.service.task.SamplingRequestOptions;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * toy 数据集端到端执行入口。
 *
 * <p>该类用于把 {@code datasets/toy/dirty.csv} 导入 FMDB 物理表，然后通过任务服务依次完成采样、标注导入、
 * 模型训练、模型发布和预测，并把每一步可核对的结果写回工程目录。默认输出目录为
 * {@code datasets/toy/raha-app-output/<runId>}。
 *
 * <p>示例：
 *
 * <pre>
 * spark-submit --class com.fiberhome.ml.raha.app.RahaToyEndToEndApp target/fmdb-udf-raha-*-all.jar
 * spark-submit --class com.fiberhome.ml.raha.app.RahaToyEndToEndApp target/fmdb-udf-raha-*-all.jar \
 *   --dirty=datasets/toy/dirty.csv --clean=datasets/toy/clean.csv --output=datasets/toy/raha-app-output
 * spark-submit --class com.fiberhome.ml.raha.app.RahaToyEndToEndApp target/fmdb-udf-raha-*-all.jar \
 *   --reset-fmdb-tables=true
 * </pre>
 */
public final class RahaToyEndToEndApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(RahaToyEndToEndApp.class);

    /** toy 数据集中用于对齐 dirty.csv 和 clean.csv 的业务主键列。 */
    private static final String ROW_KEY_COLUMN = "ID";

    /** 标注模板中表示整行是否错误的系统列。 */
    private static final String ROW_LABEL_COLUMN = "_row_label";

    /** 标注模板中表示错误列集合的系统列。 */
    private static final String ERROR_COLUMNS_COLUMN = "_error_columns";

    /** 标注模板中写入自动转换说明的系统列。 */
    private static final String COMMENT_COLUMN = "_comment";

    /** 默认运行编号格式，便于输出目录和临时表名保持可读。 */
    private static final DateTimeFormatter RUN_ID_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /** Spark 会话，用于读写 CSV、创建 FMDB 表、执行现有任务工作流。 */
    private final SparkSession sparkSession;

    /** 命令行解析后的运行参数，集中保存输入、输出和表名等配置。 */
    private final AppOptions options;

    /** 当前运行的全部输出目录路径，避免各步骤自行拼接造成目录分散。 */
    private final RunPaths paths;

    /** 统一时钟，便于标注导入、模型发布和结果记录使用一致时间源。 */
    private final Clock clock;

    private RahaToyEndToEndApp(SparkSession sparkSession, AppOptions options, Clock clock) {
        this.sparkSession = sparkSession;
        this.options = options;
        this.clock = clock;
        this.paths = RunPaths.create(options.outputBaseDirectory.resolve(options.runId));
    }

    /**
     * 命令行入口。
     *
     * @param args 可选参数包括 dirty、clean、output、table、run-id、master、labeling-budget、reset-fmdb-tables
     */
    public static void main(String[] args) {
        AppOptions options = AppOptions.parse(args);
        System.setProperty("raha.runtime.storage-mode", RahaStorageMode.FMDB.name());
        SparkSession sparkSession = createSparkSession(options);
        try {
            RahaToyEndToEndApp app =
                new RahaToyEndToEndApp(sparkSession, options, Clock.systemUTC());
            app.run();
        } finally {
            if (options.stopSparkSession) {
                sparkSession.stop();
            }
        }
    }

    /**
     * 执行 toy 数据集的采样、标注、训练、发布和预测主流程。
     */
    public void run() {
        LOGGER.info("开始执行 toy 数据集端到端流程，runId={}, output={}", options.runId, paths.runDirectory);
        try {
            paths.createDirectories();
            copyInputFiles();
            resetFmdbTablesIfRequested();

            String tableName = createDirtySourceTable();
            FmdbPersistenceConfig persistenceConfig = FmdbPersistenceConfig.fromDefaults();
            FmdbTableGateway tableGateway = new SparkSqlFmdbTableGateway(sparkSession, persistenceConfig);
            SampleRecordRepository sampleRepository =
                new FmdbSampleRecordRepository(sparkSession, tableGateway, persistenceConfig);
            AnnotationRecordRepository annotationRepository =
                new FmdbAnnotationRecordRepository(sparkSession, tableGateway, persistenceConfig);

            RahaTaskApplicationService taskService =
                RahaTaskApplicationServiceFactory.createDefault(sparkSession, paths.modelDirectory, RahaStorageMode.FMDB);
            RahaTaskRequestFactory requestFactory = taskService.getRequestFactory();
            FmdbInputSpec dirtyInput = FmdbInputSpec.table(tableName)
                .withRowKeyColumns(ROW_KEY_COLUMN)
                .withVersion(null, options.runId);

            RahaSampleOutput sampleOutput = executeSampling(taskService, requestFactory, dirtyInput);
            AnnotationBatch annotationBatch =
                createAndImportAnnotation(sampleRepository, annotationRepository, sampleOutput.getSampleBatch());
            TrainingStepResult trainingStep =
                executeTraining(taskService, requestFactory, sampleOutput.getSampleBatch());
            List<RahaColumnModel> publishedModels =
                publishCandidateModels(tableGateway, persistenceConfig, trainingStep);
            RahaDetectOutput detectOutput = executeDetection(taskService, requestFactory,
                dirtyInput, trainingStep, publishedModels, tableGateway, persistenceConfig);

            exportPhysicalTables();
            writeRunSummary(tableName, sampleOutput, annotationBatch, trainingStep.output, publishedModels, detectOutput);
            LOGGER.info("toy 数据集端到端流程执行完成，输出目录={}", paths.runDirectory);
            System.out.println("RAHA_APP_OUTPUT=" + paths.runDirectory.toAbsolutePath().normalize());
        } catch (RuntimeException | IOException ex) {
            LOGGER.error("toy 数据集端到端流程执行失败，runId={}, output={}", options.runId, paths.runDirectory, ex);
            throw new IllegalStateException("toy 数据集端到端流程执行失败，请查看日志和输出目录: " + paths.runDirectory, ex);
        }
    }

    /**
     * 创建 Spark 会话。
     *
     * @param options 运行参数
     * @return Spark 会话
     */
    private static SparkSession createSparkSession(AppOptions options) {
        SparkSession.Builder builder = SparkSession.builder()
            .appName("raha-toy-end-to-end")
            .config("spark.sql.shuffle.partitions", "4");
        if (options.sparkMaster != null && !options.sparkMaster.trim().isEmpty()) {
            builder.master(options.sparkMaster);
        }
        LOGGER.info("初始化 SparkSession，master={}", options.sparkMaster == null ? "default" : options.sparkMaster);
        return builder.enableHiveSupport().getOrCreate();
    }

    /**
     * 将输入文件复制到本次运行目录，便于核对原始数据和人工标注数据。
     */
    private void copyInputFiles() throws IOException {
        LOGGER.info("复制 toy 输入文件，dirty={}, clean={}", options.dirtyCsvPath, options.cleanCsvPath);
        Files.copy(options.dirtyCsvPath, paths.inputDirectory.resolve("dirty.csv"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Files.copy(options.cleanCsvPath, paths.inputDirectory.resolve("clean.csv"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * 在显式开启核验开关时重建 Raha 标准 FMDB 物理表。
     *
     * <p>该操作会删除 {@code dw.raha_*} 标准物理表中的历史数据，只允许用于本地或容器核验。
     * 默认关闭，避免普通运行误删 FMDB 中已有的业务结果。</p>
     */
    private void resetFmdbTablesIfRequested() {
        if (!options.resetFmdbTables) {
            return;
        }
        LOGGER.warn("已开启 FMDB 标准物理表重建开关，本次运行会删除 Raha 标准表历史数据");
        sparkSession.sql("CREATE DATABASE IF NOT EXISTS dw");
        for (FmdbPhysicalTable physicalTable : FmdbPhysicalTable.values()) {
            String tableName = physicalTable.getTableName();
            LOGGER.warn("删除并等待后续初始化重建 FMDB 标准物理表，table={}", tableName);
            sparkSession.sql("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * 把 dirty.csv 导入 Spark 物理表，并额外导出一份 CSV 结果用于核对。
     *
     * @return dirty 数据对应的 Spark 表名
     */
    private String createDirtySourceTable() throws IOException {
        ensureInputFile(options.dirtyCsvPath, "dirty.csv");
        createDatabaseIfNeeded(options.sourceTableName);
        LOGGER.info("读取 dirty.csv 并写入 Spark 表，table={}", options.sourceTableName);
        Dataset<Row> dirtyFrame = sparkSession.read()
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(toSparkPath(options.dirtyCsvPath));
        if (!Arrays.asList(dirtyFrame.columns()).contains(ROW_KEY_COLUMN)) {
            throw new IllegalArgumentException("dirty.csv 缺少业务主键列: " + ROW_KEY_COLUMN);
        }
        dirtyFrame.write().mode(SaveMode.Overwrite).saveAsTable(options.sourceTableName);
        Dataset<Row> persistedFrame = sparkSession.table(options.sourceTableName);
        long rowCount = persistedFrame.count();
        LOGGER.info("dirty.csv 已写入 Spark 表，table={}, rows={}", options.sourceTableName, rowCount);
        exportDatasetAsCsv(persistedFrame, paths.inputDirectory.resolve("dirty-table-csv"));
        writeJson(paths.inputDirectory.resolve("dirty-table.json"), mapOf(
            "tableName", options.sourceTableName,
            "rowCount", rowCount,
            "exportDirectory", paths.inputDirectory.resolve("dirty-table-csv").toString()));
        return options.sourceTableName;
    }

    /**
     * 执行采样任务，并将采样批次和采样记录写入输出目录。
     */
    private RahaSampleOutput executeSampling(
        RahaTaskApplicationService taskService,
        RahaTaskRequestFactory requestFactory,
        FmdbInputSpec dirtyInput) throws IOException {

        LOGGER.info("开始采样任务，input={}, labelingBudget={}", dirtyInput.getInputReference(), options.labelingBudget);
        SamplingRequestOptions samplingOptions =
            new SamplingRequestOptions(options.labelingBudget, 1, Collections.<CellLabel>emptyList());
        RahaTaskExecutionRequest request = requestFactory.sampling(dirtyInput, samplingOptions);
        RahaTaskExecutionResult result = requireSuccessfulTask(taskService.execute(request), "采样");
        RahaSampleOutput output = requirePayload(result, RahaSampleOutput.class, "采样");
        SampleBatch sampleBatch = output.getSampleBatch();
        if (sampleBatch == null) {
            throw new IllegalStateException("采样任务未返回采样批次，无法继续标注和训练");
        }
        writeSamplingOutputs(result, output);
        LOGGER.info("采样任务完成，sampleBatchId={}, sampledRows={}",
            sampleBatch.getSampleBatchId(), sampleBatch.getRecords().size());
        return output;
    }

    /**
     * 导出标注模板，将 clean.csv 自动转换为工程模板，再导入标注记录。
     */
    private AnnotationBatch createAndImportAnnotation(
        SampleRecordRepository sampleRepository,
        AnnotationRecordRepository annotationRepository,
        SampleBatch sampleBatch) throws IOException {

        ensureInputFile(options.cleanCsvPath, "clean.csv");
        LOGGER.info("开始生成并导入标注，sampleBatchId={}", sampleBatch.getSampleBatchId());
        AnnotationWorkbookAdapter workbookAdapter =
            new AnnotationWorkbookAdapter(AnnotationExcelConfig.defaults());
        AnnotationTemplateService templateService =
            new AnnotationTemplateService(sampleRepository, workbookAdapter, clock);
        templateService.exportTemplate(new AnnotationTemplateRequest(
            sampleBatch.getDatasetId(),
            sampleBatch.getPartitionMonth(),
            sampleBatch.getSampleBatchId(),
            paths.annotationTemplateFile));

        Map<String, Map<String, String>> cleanRows = loadCleanRows();
        fillAnnotationWorkbook(paths.annotationTemplateFile, paths.annotationFilledTemplateFile, cleanRows);

        AnnotationImportService importService = new AnnotationImportService(
            sampleRepository,
            annotationRepository,
            workbookAdapter,
            new AnnotationLabelExpander(),
            clock);
        AnnotationImportResult importResult = importService.importAnnotations(new AnnotationImportRequest(
            sampleBatch.getDatasetId(),
            sampleBatch.getPartitionMonth(),
            sampleBatch.getSampleBatchId(),
            paths.annotationFilledTemplateFile,
            paths.annotationValidationFile,
            "toy-clean-csv",
            false,
            null,
            null));

        writeAnnotationOutputs(importResult);
        if (importResult.getBatch() == null) {
            throw new IllegalStateException("标注导入没有生成标注批次，状态=" + importResult.getStatus());
        }
        LOGGER.info("标注导入完成，annotationBatchId={}, status={}",
            importResult.getBatch().getAnnotationBatchId(), importResult.getStatus());
        return importResult.getBatch();
    }

    /**
     * 执行训练任务，并把训练结果摘要写入输出目录。
     */
    private TrainingStepResult executeTraining(
        RahaTaskApplicationService taskService,
        RahaTaskRequestFactory requestFactory,
        SampleBatch sampleBatch) throws IOException {

        LOGGER.info("开始训练任务，sampleBatchId={}", sampleBatch.getSampleBatchId());
        RahaTaskExecutionRequest request = requestFactory.training(sampleBatch.getSampleBatchId());
        RahaTaskExecutionResult result = requireSuccessfulTask(taskService.execute(request), "训练");
        RahaTrainOutput output = requirePayload(result, RahaTrainOutput.class, "训练");
        if (output.getCandidateModels().isEmpty()) {
            throw new IllegalStateException("训练未生成候选模型，无法发布和预测");
        }
        writeTrainingOutputs(result, output);
        LOGGER.info("训练任务完成，modelSetVersion={}, candidateModels={}",
            output.getModelSetVersion(), output.getCandidateModels().size());
        return new TrainingStepResult(result, output);
    }

    /**
     * 将训练产生的候选模型发布为可检测模型。
     */
    private List<RahaColumnModel> publishCandidateModels(
        FmdbTableGateway tableGateway,
        FmdbPersistenceConfig persistenceConfig,
        TrainingStepResult trainingStep) throws IOException {

        RahaTrainOutput trainOutput = trainingStep.output;
        LOGGER.info("开始发布候选模型，modelSetVersion={}", trainOutput.getModelSetVersion());
        ModelMetadataRepository modelRepository =
            new FmdbModelMetadataRepository(sparkSession, tableGateway, persistenceConfig);
        ModelReleaseManager releaseManager = new ModelReleaseManager(modelRepository, clock);
        ArtifactVersion publishArtifactVersion = new ArtifactVersion(
            trainingStep.taskResult.getJob().getConfigVersion(),
            trainingStep.taskResult.getJob().getSnapshotId(),
            "app-publish",
            0);

        List<RahaColumnModel> publishedModels = new ArrayList<RahaColumnModel>();
        for (RahaColumnModel candidateModel : trainOutput.getCandidateModels().values()) {
            // 预测工作流只会加载已发布模型，因此训练完成后必须显式发布候选模型。
            RahaColumnModel publishedModel = releaseManager.publish(
                candidateModel.getDatasetId(),
                candidateModel.getColumnName(),
                candidateModel.getModelVersion(),
                publishArtifactVersion);
            publishedModels.add(publishedModel);
        }
        writeModelOutputs(trainOutput, publishedModels);
        LOGGER.info("模型发布完成，publishedModels={}", publishedModels.size());
        return publishedModels;
    }

    /**
     * 执行预测任务，并导出预测单元格结果。
     */
    private RahaDetectOutput executeDetection(
        RahaTaskApplicationService taskService,
        RahaTaskRequestFactory requestFactory,
        FmdbInputSpec dirtyInput,
        TrainingStepResult trainingStep,
        List<RahaColumnModel> publishedModels,
        FmdbTableGateway tableGateway,
        FmdbPersistenceConfig persistenceConfig) throws IOException {

        RahaTrainOutput trainOutput = trainingStep.output;
        LOGGER.info("开始预测任务，modelSetVersion={}", trainOutput.getModelSetVersion());
        DetectionRequestOptions detectionOptions =
            new DetectionRequestOptions(MissingModelPolicy.PARTIAL);
        RahaTaskExecutionRequest request =
            requestFactory.detection(dirtyInput, trainOutput.getModelSetVersion(), detectionOptions);
        RahaTaskExecutionResult result = taskService.execute(request);
        RahaDetectOutput output;
        if (isSuccessfulTask(result)) {
            output = requirePayload(result, RahaDetectOutput.class, "预测");
            writeDetectionOutputs(result, output);
            LOGGER.info("预测任务完成，detectedCells={}, failedColumns={}",
                output.getResults().size(), output.getFailedColumns().size());
            return output;
        }

        LOGGER.warn("标准预测任务未成功，启用训练特征回放预测，status={}，errorCode={}，errorMessage={}",
            result == null || result.getJob() == null ? null : result.getJob().getStatus(),
            result == null || result.getJob() == null ? null : result.getJob().getErrorCode(),
            result == null || result.getJob() == null ? null : result.getJob().getErrorMessage());
        output = executeTrainingFeatureReplayDetection(
            dirtyInput, trainingStep, publishedModels, tableGateway, persistenceConfig);
        writeDetectionOutputs(result, output, true,
            "标准预测任务未成功，已使用训练阶段冻结特征回放预测");
        LOGGER.info("训练特征回放预测完成，detectedCells={}, failedColumns={}",
            output.getResults().size(), output.getFailedColumns().size());
        return output;
    }

    /**
     * 使用训练阶段冻结特征和已发布模型执行同数据集回放预测。
     *
     * <p>该兜底只服务于 toy 端到端核验：标准检测任务如果因为当前特征字典版本与训练版本不一致失败，
     * 仍然可以复用训练时已经冻结的特征行和模型产物生成可核对预测结果。</p>
     */
    private RahaDetectOutput executeTrainingFeatureReplayDetection(
        FmdbInputSpec dirtyInput,
        TrainingStepResult trainingStep,
        List<RahaColumnModel> publishedModels,
        FmdbTableGateway tableGateway,
        FmdbPersistenceConfig persistenceConfig) {

        String detectionBatchId = "app-detect-" + options.runId;
        Map<String, RahaColumnModel> publishedByColumn = modelsByColumn(publishedModels);
        List<DetectionResult> results = new ArrayList<DetectionResult>();
        Map<String, String> modelVersions = new LinkedHashMap<String, String>();
        Map<String, String> failedColumns = new LinkedHashMap<String, String>();
        ColumnModelPredictor predictor = new ColumnModelPredictor();
        for (Map.Entry<String, FeatureDictionary> entry
                : trainingStep.output.getFeatures().getDictionaries().entrySet()) {
            String columnName = entry.getKey();
            RahaColumnModel publishedModel = publishedByColumn.get(columnName);
            com.fiberhome.ml.raha.model.training.ColumnModelTrainingResult trainingResult =
                trainingStep.output.getTrainingResults().get(columnName);
            if (publishedModel == null || trainingResult == null
                    || trainingResult.getArtifact() == null) {
                failedColumns.put(columnName, "NO_PUBLISHED_MODEL");
                continue;
            }

            ColumnModelArtifact model = trainingResult.getArtifact()
                .withThreshold(publishedModel.getThreshold());
            List<SparseFeatureRow> featureRows =
                trainingStep.output.getFeatures().getRowsByColumn(columnName);
            List<ColumnPrediction> predictions = predictor.predict(model, featureRows);
            for (int index = 0; index < featureRows.size(); index++) {
                SparseFeatureRow featureRow = featureRows.get(index);
                ColumnPrediction prediction = predictions.get(index);
                Map<String, String> reasons = new LinkedHashMap<String, String>();
                reasons.put("classifierType", model.getClassifierType().name());
                reasons.put("trainingMode", model.getTrainingMode());
                reasons.put("predictionSource", "TRAINING_FEATURE_REPLAY");
                results.add(new DetectionResult(
                    detectionBatchId,
                    trainingStep.taskResult.getJob().getConfigVersion(),
                    "app-training-feature-detection",
                    featureRow.getCoordinate(),
                    featureRow.getValueHash(),
                    featureRow.getMaskedValue(),
                    prediction.isError(),
                    prediction.getScore(),
                    prediction.getThreshold(),
                    strategyIds(entry.getValue(), featureRow),
                    reasons,
                    model.getModelName(),
                    model.getModelVersion(),
                    model.getFeatureDictionaryVersion(),
                    clock.millis()));
            }
            modelVersions.put(columnName, model.getModelVersion());
        }
        if (modelVersions.isEmpty()) {
            throw new IllegalStateException("训练特征回放预测没有可用模型");
        }
        SparkSqlFmdbResultWriter resultWriter =
            new SparkSqlFmdbResultWriter(sparkSession, tableGateway, clock, persistenceConfig);
        long written = resultWriter.writeDetectionResults(
            FmdbPhysicalTable.DETECTION_RESULT.getTableName(),
            new FmdbDetectionWriteContext(
                detectionBatchId,
                dirtyInput.getInputReference(),
                trainingStep.output.getModelSetVersion(),
                trustedRowsByRowId(dirtyInput.getInputReference())),
            results);
        LOGGER.info("训练特征回放预测已写入 FMDB 错误表，detectionBatchId={}，writtenCount={}",
            detectionBatchId, written);
        return new RahaDetectOutput(results, modelVersions, failedColumns);
    }

    /**
     * 按字段名索引已发布模型，便于预测时找到对应模型参数。
     */
    private static Map<String, RahaColumnModel> modelsByColumn(List<RahaColumnModel> models) {
        Map<String, RahaColumnModel> result = new LinkedHashMap<String, RahaColumnModel>();
        for (RahaColumnModel model : models) {
            result.put(model.getColumnName(), model);
        }
        return result;
    }

    /**
     * 构建 FMDB 错误表写入所需的可信原始行索引。
     */
    private Map<String, Map<String, Object>> trustedRowsByRowId(String tableName) {
        Dataset<Row> identified = new RowIdentityService()
            .identify(sparkSession.table(tableName), RowIdentityConfig.sourceKey(ROW_KEY_COLUMN))
            .getDataFrame();
        Map<String, Map<String, Object>> rowsById = new LinkedHashMap<String, Map<String, Object>>();
        for (Row row : identified.collectAsList()) {
            String rowId = String.valueOf((Object) row.getAs(RowIdentityColumns.ROW_ID));
            Map<String, Object> values = new LinkedHashMap<String, Object>();
            for (StructField field : identified.schema().fields()) {
                if (!RowIdentityColumns.isTechnical(field.name())) {
                    values.put(field.name(), row.getAs(field.name()));
                }
            }
            if (rowsById.put(rowId, values) != null) {
                throw new IllegalStateException("检测输入包含重复逻辑行：" + rowId);
            }
        }
        return rowsById;
    }

    /**
     * 加载 clean.csv，并以业务主键列建立行映射。
     */
    private Map<String, Map<String, String>> loadCleanRows() {
        LOGGER.info("读取 clean.csv，用于转换工程标注模板，path={}", options.cleanCsvPath);
        Dataset<Row> cleanFrame = sparkSession.read()
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(toSparkPath(options.cleanCsvPath));
        List<String> columns = Arrays.asList(cleanFrame.columns());
        if (!columns.contains(ROW_KEY_COLUMN)) {
            throw new IllegalArgumentException("clean.csv 缺少业务主键列: " + ROW_KEY_COLUMN);
        }
        Map<String, Map<String, String>> rowsByKey = new LinkedHashMap<String, Map<String, String>>();
        for (Row row : cleanFrame.collectAsList()) {
            String key = normalizeValue(row.getAs(ROW_KEY_COLUMN));
            if (key.isEmpty()) {
                throw new IllegalArgumentException("clean.csv 存在空业务主键，无法与采样结果对齐");
            }
            Map<String, String> values = new LinkedHashMap<String, String>();
            for (String column : columns) {
                values.put(column, normalizeValue(row.getAs(column)));
            }
            rowsByKey.put(key, values);
        }
        LOGGER.info("clean.csv 加载完成，rows={}", rowsByKey.size());
        return rowsByKey;
    }

    /**
     * 根据 clean.csv 填充标注模板，生成可导入的 xls 文件。
     */
    private void fillAnnotationWorkbook(
        Path templateFile,
        Path filledTemplateFile,
        Map<String, Map<String, String>> cleanRows) throws IOException {

        LOGGER.info("填充标注模板，template={}, output={}", templateFile, filledTemplateFile);
        try (InputStream inputStream = Files.newInputStream(templateFile);
             Workbook workbook = new HSSFWorkbook(inputStream);
             OutputStream outputStream = Files.newOutputStream(filledTemplateFile)) {
            Sheet sheet = workbook.getSheet(AnnotationWorkbookAdapter.DATA_SHEET);
            if (sheet == null) {
                throw new IllegalStateException("标注模板缺少数据页: " + AnnotationWorkbookAdapter.DATA_SHEET);
            }
            org.apache.poi.ss.usermodel.Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                throw new IllegalStateException("标注模板缺少表头行");
            }
            DataFormatter formatter = new DataFormatter(Locale.ROOT);
            Map<String, Integer> headerIndexes = readHeaderIndexes(headerRow, formatter);
            List<String> businessColumns = resolveBusinessColumns(headerIndexes);
            requireHeader(headerIndexes, ROW_KEY_COLUMN);
            requireHeader(headerIndexes, ROW_LABEL_COLUMN);
            requireHeader(headerIndexes, ERROR_COLUMNS_COLUMN);
            requireHeader(headerIndexes, COMMENT_COLUMN);

            int filledRows = 0;
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                org.apache.poi.ss.usermodel.Row row = sheet.getRow(rowIndex);
                if (row == null) {
                    continue;
                }
                String rowKey = cellText(row, requireHeader(headerIndexes, ROW_KEY_COLUMN), formatter);
                Map<String, String> cleanValues = cleanRows.get(normalizeValue(rowKey));
                if (cleanValues == null) {
                    throw new IllegalStateException("clean.csv 中找不到采样行对应的业务主键: " + rowKey);
                }
                List<String> errorColumns = findErrorColumns(row, businessColumns, cleanValues, headerIndexes, formatter);
                // 标注模板使用 1 表示错误行，0 表示正确行；错误列为空时保留空字符串。
                setCellText(row, requireHeader(headerIndexes, ROW_LABEL_COLUMN), errorColumns.isEmpty() ? "0" : "1");
                setCellText(row, requireHeader(headerIndexes, ERROR_COLUMNS_COLUMN), join(errorColumns, ","));
                setCellText(row, requireHeader(headerIndexes, COMMENT_COLUMN), "toy clean.csv 自动转换");
                filledRows++;
            }
            workbook.write(outputStream);
            LOGGER.info("标注模板填充完成，rows={}", filledRows);
        }
    }

    /**
     * 对比 dirty 采样值与 clean 人工标注值，返回需要标错的业务列。
     */
    private List<String> findErrorColumns(
        org.apache.poi.ss.usermodel.Row templateRow,
        List<String> businessColumns,
        Map<String, String> cleanValues,
        Map<String, Integer> headerIndexes,
        DataFormatter formatter) {

        List<String> errorColumns = new ArrayList<String>();
        for (String businessColumn : businessColumns) {
            Integer columnIndex = headerIndexes.get(businessColumn);
            String dirtyValue = cellText(templateRow, columnIndex.intValue(), formatter);
            String cleanValue = normalizeValue(cleanValues.get(businessColumn));
            // clean.csv 是人工标注后的正确值；两侧不一致时，该单元格就是候选错误。
            if (!normalizeValue(dirtyValue).equals(cleanValue)) {
                errorColumns.add(businessColumn);
            }
        }
        return errorColumns;
    }

    /**
     * 读取模板表头和列下标。
     */
    private Map<String, Integer> readHeaderIndexes(
        org.apache.poi.ss.usermodel.Row headerRow,
        DataFormatter formatter) {

        Map<String, Integer> headerIndexes = new LinkedHashMap<String, Integer>();
        for (int columnIndex = 0; columnIndex < headerRow.getLastCellNum(); columnIndex++) {
            String headerName = cellText(headerRow, columnIndex, formatter);
            if (!headerName.isEmpty()) {
                headerIndexes.put(headerName, columnIndex);
            }
        }
        return headerIndexes;
    }

    /**
     * 解析模板中的业务列，系统标注列之外的列都会参与 dirty 与 clean 的比对。
     */
    private List<String> resolveBusinessColumns(Map<String, Integer> headerIndexes) {
        int rowLabelIndex = requireHeader(headerIndexes, ROW_LABEL_COLUMN);
        List<String> businessColumns = new ArrayList<String>();
        for (Map.Entry<String, Integer> entry : headerIndexes.entrySet()) {
            if (entry.getValue().intValue() < rowLabelIndex && !entry.getKey().startsWith("_")) {
                businessColumns.add(entry.getKey());
            }
        }
        if (businessColumns.isEmpty()) {
            throw new IllegalStateException("标注模板未识别到业务列");
        }
        return businessColumns;
    }

    /**
     * 导出采样任务结果。
     */
    private void writeSamplingOutputs(RahaTaskExecutionResult result, RahaSampleOutput output) throws IOException {
        SampleBatch sampleBatch = output.getSampleBatch();
        writeJson(paths.samplingDirectory.resolve("task-result.json"), taskResultSummary(result));
        writeJson(paths.samplingDirectory.resolve("sample-output.json"), mapOf(
            "sampleBatchId", sampleBatch.getSampleBatchId(),
            "datasetId", sampleBatch.getDatasetId(),
            "snapshotId", sampleBatch.getSnapshotId(),
            "sourceVersion", sampleBatch.getSourceVersion(),
            "samplingVersion", sampleBatch.getSamplingVersion(),
            "partitionMonth", sampleBatch.getPartitionMonth(),
            "recordCount", sampleBatch.getRecords().size()));

        List<String> headers = new ArrayList<String>();
        headers.add("sample_batch_id");
        headers.add("dataset_id");
        headers.add("snapshot_id");
        headers.add("partition_month");
        headers.add("row_id");
        headers.add("row_content_hash");
        headers.add("annotation_task_id");
        headers.addAll(resolveRowDataColumns(sampleBatch.getRecords()));
        headers.add("row_data_json");

        List<List<String>> rows = new ArrayList<List<String>>();
        for (SampleRecord record : sampleBatch.getRecords()) {
            List<String> row = new ArrayList<String>();
            row.add(sampleBatch.getSampleBatchId());
            row.add(record.getDatasetId());
            row.add(sampleBatch.getSnapshotId());
            row.add(record.getPartitionMonth());
            row.add(record.getRowId());
            row.add(record.getRowContentHash());
            row.add(String.valueOf(record.getSamplingContext().get("annotationTaskId")));
            for (String column : resolveRowDataColumns(sampleBatch.getRecords())) {
                row.add(normalizeValue(record.getRowData().get(column)));
            }
            row.add(FmdbJsonCodec.write(record.getRowData()));
            rows.add(row);
        }
        writeCsv(paths.samplingDirectory.resolve("sample-records.csv"), headers, rows);
    }

    /**
     * 导出标注导入结果。
     */
    private void writeAnnotationOutputs(AnnotationImportResult importResult) throws IOException {
        AnnotationBatch batch = importResult.getBatch();
        writeJson(paths.annotationDirectory.resolve("import-result.json"), mapOf(
            "status", String.valueOf(importResult.getStatus()),
            "annotationBatchId", batch == null ? null : batch.getAnnotationBatchId(),
            "sampleBatchId", batch == null ? null : batch.getSampleBatchId(),
            "datasetId", batch == null ? null : batch.getDatasetId(),
            "partitionMonth", batch == null ? null : batch.getPartitionMonth(),
            "labels", importResult.getCellLabels().size(),
            "errors", importResult.getErrors().size(),
            "validationFile", Files.exists(paths.annotationValidationFile) ? paths.annotationValidationFile.toString() : null));

        if (batch == null) {
            writeCsv(paths.annotationDirectory.resolve("annotation-records.csv"),
                Arrays.asList("status", "message"),
                Collections.singletonList(Arrays.asList(String.valueOf(importResult.getStatus()), "没有生成标注批次")));
            return;
        }

        List<String> headers = Arrays.asList(
            "annotation_batch_id",
            "sample_batch_id",
            "dataset_id",
            "partition_month",
            "row_id",
            "row_label",
            "error_columns",
            "comment",
            "annotator",
            "created_at");
        List<List<String>> rows = new ArrayList<List<String>>();
        for (com.fiberhome.ml.raha.annotation.domain.AnnotationRecord record : batch.getRecords()) {
            rows.add(Arrays.asList(
                record.getAnnotationBatchId(),
                record.getSampleBatchId(),
                record.getDatasetId(),
                record.getPartitionMonth(),
                record.getAnnotation().getRowId(),
                String.valueOf(record.getAnnotation().getRowLabel()),
                join(record.getAnnotation().getErrorColumns(), ","),
                record.getAnnotation().getComment(),
                record.getAnnotator(),
                String.valueOf(record.getAnnotatedAt())));
        }
        writeCsv(paths.annotationDirectory.resolve("annotation-records.csv"), headers, rows);
    }

    /**
     * 导出训练任务结果。
     */
    private void writeTrainingOutputs(RahaTaskExecutionResult result, RahaTrainOutput output) throws IOException {
        writeJson(paths.trainingDirectory.resolve("task-result.json"), taskResultSummary(result));
        writeJson(paths.trainingDirectory.resolve("training-output.json"), mapOf(
            "modelSetVersion", output.getModelSetVersion(),
            "candidateModels", output.getCandidateModels().size(),
            "trainingResults", output.getTrainingResults().size(),
            "materialized", output.getMaterializationResult() != null));
    }

    /**
     * 导出模型清单，模型文件本体由 FMDB 模型存储写入 model 目录。
     */
    private void writeModelOutputs(RahaTrainOutput trainOutput, List<RahaColumnModel> publishedModels) throws IOException {
        writeJson(paths.modelDirectory.resolve("model-output.json"), mapOf(
            "modelSetVersion", trainOutput.getModelSetVersion(),
            "candidateModels", trainOutput.getCandidateModels().size(),
            "publishedModels", publishedModels.size(),
            "modelDirectory", paths.modelDirectory.toString()));

        writeCsv(paths.modelDirectory.resolve("candidate-models.csv"),
            modelHeaders(),
            modelRows(trainOutput.getCandidateModels().values(), trainOutput.getModelSetVersion()));
        writeCsv(paths.modelDirectory.resolve("published-models.csv"),
            modelHeaders(),
            modelRows(publishedModels, trainOutput.getModelSetVersion()));
        writeModelArtifactFiles(trainOutput, publishedModels);
    }

    /**
     * 将 FMDB 模式中的模型参数额外导出为 JSON 文件，便于离线核对模型系数和阈值。
     */
    private void writeModelArtifactFiles(
        RahaTrainOutput trainOutput,
        List<RahaColumnModel> publishedModels) throws IOException {

        Map<String, RahaColumnModel> publishedByColumn = modelsByColumn(publishedModels);
        for (Map.Entry<String, com.fiberhome.ml.raha.model.training.ColumnModelTrainingResult> entry
                : trainOutput.getTrainingResults().entrySet()) {
            ColumnModelArtifact artifact = entry.getValue().getArtifact();
            if (artifact == null) {
                continue;
            }
            RahaColumnModel candidate = trainOutput.getCandidateModels().get(entry.getKey());
            RahaColumnModel published = publishedByColumn.get(entry.getKey());
            writeJson(paths.modelDirectory.resolve("artifacts")
                    .resolve(artifact.getModelVersion() + ".json"), mapOf(
                "modelSetVersion", trainOutput.getModelSetVersion(),
                "datasetId", candidate == null ? null : candidate.getDatasetId(),
                "columnName", artifact.getColumnName(),
                "modelName", artifact.getModelName(),
                "modelVersion", artifact.getModelVersion(),
                "status", published == null ? "CANDIDATE" : String.valueOf(published.getStatus()),
                "classifierType", String.valueOf(artifact.getClassifierType()),
                "featureDictionaryVersion", artifact.getFeatureDictionaryVersion(),
                "featureDimension", artifact.getFeatureDimension(),
                "threshold", published == null ? artifact.getThreshold() : published.getThreshold(),
                "intercept", artifact.getIntercept(),
                "coefficients", artifact.getCoefficients(),
                "trainingMode", artifact.getTrainingMode(),
                "metrics", entry.getValue().getMetrics(),
                "modelPath", candidate == null ? null : candidate.getModelPath(),
                "publishedAt", published == null ? null : published.getPublishedAt()));
        }
    }

    /**
     * 导出预测任务结果。
     */
    private void writeDetectionOutputs(RahaTaskExecutionResult result, RahaDetectOutput output) throws IOException {
        writeDetectionOutputs(result, output, false, null);
    }

    /**
     * 导出预测任务结果，并标记是否启用了 app 层兜底预测。
     */
    private void writeDetectionOutputs(
        RahaTaskExecutionResult result,
        RahaDetectOutput output,
        boolean fallbackUsed,
        String fallbackReason) throws IOException {

        writeJson(paths.detectionDirectory.resolve("task-result.json"), taskResultSummary(result));
        writeJson(paths.detectionDirectory.resolve("detection-output.json"), mapOf(
            "resultCount", output.getResults().size(),
            "modelVersions", output.getModelVersions(),
            "failedColumns", output.getFailedColumns(),
            "fallbackUsed", fallbackUsed,
            "fallbackReason", fallbackReason));

        List<String> headers = Arrays.asList(
            "dataset_id",
            "snapshot_id",
            "row_id",
            "column_name",
            "score",
            "threshold",
            "detected_as_error",
            "model_name",
            "model_version",
            "strategy_ids",
            "reasons_json");
        List<List<String>> rows = new ArrayList<List<String>>();
        for (DetectionResult detectionResult : output.getResults()) {
            rows.add(Arrays.asList(
                detectionResult.getCoordinate().getDatasetId(),
                detectionResult.getCoordinate().getSnapshotId(),
                detectionResult.getCoordinate().getRowId(),
                detectionResult.getCoordinate().getColumnName(),
                String.valueOf(detectionResult.getScore()),
                String.valueOf(detectionResult.getThreshold()),
                String.valueOf(detectionResult.isError()),
                detectionResult.getModelName(),
                detectionResult.getModelVersion(),
                join(detectionResult.getStrategyIds(), ","),
                FmdbJsonCodec.write(detectionResult.getReasons())));
        }
        writeCsv(paths.detectionDirectory.resolve("detected-cells.csv"), headers, rows);
    }

    /**
     * 导出 FMDB 标准物理表，每张表一个 CSV 目录，便于逐表核对。
     */
    private void exportPhysicalTables() throws IOException {
        LOGGER.info("开始导出 FMDB 物理表，output={}", paths.fmdbTablesDirectory);
        for (FmdbPhysicalTable physicalTable : FmdbPhysicalTable.values()) {
            String tableName = physicalTable.getTableName();
            Path targetDirectory = paths.fmdbTablesDirectory.resolve(safePathName(tableName));
            try {
                // 物理表可能在某些流程未触发时不存在，此时记录缺失文件而不中断主流程。
                if (!sparkSession.catalog().tableExists(tableName)) {
                    writeText(targetDirectory.resolve("_MISSING.txt"), "表不存在: " + tableName);
                    LOGGER.warn("跳过不存在的 FMDB 物理表，table={}", tableName);
                    continue;
                }
                Dataset<Row> tableFrame = sparkSession.table(tableName);
                long rowCount = tableFrame.count();
                exportDatasetAsCsv(tableFrame, targetDirectory);
                writeText(targetDirectory.resolve("_ROW_COUNT.txt"), String.valueOf(rowCount));
                LOGGER.info("FMDB 物理表导出完成，table={}, rows={}, output={}", tableName, rowCount, targetDirectory);
            } catch (RuntimeException ex) {
                LOGGER.error("FMDB 物理表导出失败，table={}, output={}", tableName, targetDirectory, ex);
                throw ex;
            }
        }
    }

    /**
     * 写入本次端到端执行总摘要。
     */
    private void writeRunSummary(
        String tableName,
        RahaSampleOutput sampleOutput,
        AnnotationBatch annotationBatch,
        RahaTrainOutput trainOutput,
        List<RahaColumnModel> publishedModels,
        RahaDetectOutput detectOutput) throws IOException {

        writeJson(paths.runDirectory.resolve("run-summary.json"), mapOf(
            "runId", options.runId,
            "dirtyCsv", options.dirtyCsvPath.toString(),
            "cleanCsv", options.cleanCsvPath.toString(),
            "sourceTableName", tableName,
            "sampleBatchId", sampleOutput.getSampleBatch().getSampleBatchId(),
            "annotationBatchId", annotationBatch.getAnnotationBatchId(),
            "modelSetVersion", trainOutput.getModelSetVersion(),
            "publishedModels", publishedModels.size(),
            "detectedCells", detectOutput.getResults().size(),
            "outputDirectory", paths.runDirectory.toString(),
            "fmdbTablesDirectory", paths.fmdbTablesDirectory.toString(),
            "modelDirectory", paths.modelDirectory.toString(),
            "finishedAt", Instant.now(clock).toString()));
    }

    /**
     * 根据任务状态判断是否可以继续后续流程。
     */
    private static RahaTaskExecutionResult requireSuccessfulTask(RahaTaskExecutionResult result, String stepName) {
        if (result == null || result.getJob() == null) {
            throw new IllegalStateException(stepName + "任务未返回执行结果");
        }
        JobStatus status = result.getJob().getStatus();
        if (status != JobStatus.SUCCEEDED && status != JobStatus.PARTIAL_SUCCESS) {
            throw new IllegalStateException(stepName + "任务执行失败，状态=" + status
                + "，错误码=" + result.getJob().getErrorCode()
                + "，错误信息=" + result.getJob().getErrorMessage());
        }
        return result;
    }

    /**
     * 判断任务状态是否允许后续流程继续。
     */
    private static boolean isSuccessfulTask(RahaTaskExecutionResult result) {
        if (result == null || result.getJob() == null) {
            return false;
        }
        JobStatus status = result.getJob().getStatus();
        return status == JobStatus.SUCCEEDED || status == JobStatus.PARTIAL_SUCCESS;
    }

    /**
     * 从任务结果中读取强类型 payload。
     */
    private static <T> T requirePayload(RahaTaskExecutionResult result, Class<T> payloadType, String stepName) {
        T payload = result.getPayload(payloadType);
        if (payload == null) {
            throw new IllegalStateException(stepName + "任务未返回 " + payloadType.getSimpleName());
        }
        return payload;
    }

    /**
     * 将任务执行结果压缩为可读摘要，避免直接序列化 Spark 或领域对象导致输出过大。
     */
    private static Map<String, Object> taskResultSummary(RahaTaskExecutionResult result) {
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("jobId", result.getJob().getJobId());
        summary.put("idempotentKey", result.getJob().getIdempotentKey());
        summary.put("jobType", String.valueOf(result.getJob().getJobType()));
        summary.put("status", String.valueOf(result.getJob().getStatus()));
        summary.put("datasetId", result.getJob().getDatasetId());
        summary.put("snapshotId", result.getJob().getSnapshotId());
        summary.put("configVersion", result.getJob().getConfigVersion());
        summary.put("errorCode", result.getJob().getErrorCode());
        summary.put("errorMessage", result.getJob().getErrorMessage());
        List<Map<String, Object>> stages = new ArrayList<Map<String, Object>>();
        for (RahaStage stage : result.getStages()) {
            stages.add(mapOf(
                "stageId", stage.getStageId(),
                "stageType", String.valueOf(stage.getStageType()),
                "status", String.valueOf(stage.getStatus()),
                "attemptId", stage.getAttemptId(),
                "errorCode", stage.getErrorCode(),
                "errorMessage", stage.getErrorMessage()));
        }
        summary.put("stages", stages);
        return summary;
    }

    /**
     * 导出 Spark Dataset 为 CSV 目录。
     */
    private void exportDatasetAsCsv(Dataset<Row> frame, Path targetDirectory) throws IOException {
        Files.createDirectories(targetDirectory.getParent());
        LOGGER.info("导出 Dataset 为 CSV，output={}", targetDirectory);
        frame.coalesce(1)
            .write()
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv(toSparkPath(targetDirectory));
    }

    /**
     * 创建表名前缀数据库，避免 saveAsTable 写入不存在的库失败。
     */
    private void createDatabaseIfNeeded(String tableName) {
        String[] parts = tableName.split("\\.");
        if (parts.length == 2) {
            LOGGER.info("确认 Spark 数据库存在，database={}", parts[0]);
            sparkSession.sql("CREATE DATABASE IF NOT EXISTS " + parts[0]);
        }
    }

    /**
     * 校验输入文件是否存在且是普通文件。
     */
    private static void ensureInputFile(Path path, String description) {
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException(description + " 不存在或不是文件: " + path);
        }
    }

    /**
     * 获取模板指定表头下标。
     */
    private static int requireHeader(Map<String, Integer> headerIndexes, String headerName) {
        Integer index = headerIndexes.get(headerName);
        if (index == null) {
            throw new IllegalStateException("标注模板缺少表头: " + headerName);
        }
        return index.intValue();
    }

    /**
     * 读取 Excel 单元格文本。
     */
    private static String cellText(
        org.apache.poi.ss.usermodel.Row row,
        int columnIndex,
        DataFormatter formatter) {

        Cell cell = row.getCell(columnIndex);
        return cell == null ? "" : normalizeValue(formatter.formatCellValue(cell));
    }

    /**
     * 写入 Excel 单元格文本。
     */
    private static void setCellText(org.apache.poi.ss.usermodel.Row row, int columnIndex, String value) {
        Cell cell = row.getCell(columnIndex);
        if (cell == null) {
            cell = row.createCell(columnIndex);
        }
        cell.setCellValue(value == null ? "" : value);
    }

    /**
     * 统一把空值转换为空字符串，避免 Spark null 与 Excel 空单元格无法对齐。
     */
    private static String normalizeValue(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    /**
     * 收集采样记录中出现过的业务列。
     */
    private static List<String> resolveRowDataColumns(Collection<SampleRecord> records) {
        Set<String> columns = new LinkedHashSet<String>();
        for (SampleRecord record : records) {
            columns.addAll(record.getRowData().keySet());
        }
        return new ArrayList<String>(columns);
    }

    /**
     * 返回模型清单 CSV 表头。
     */
    private static List<String> modelHeaders() {
        return Arrays.asList(
            "model_set_version",
            "dataset_id",
            "column_name",
            "model_name",
            "model_version",
            "status",
            "classifier_type",
            "feature_dictionary_version",
            "strategy_plan_version",
            "threshold",
            "model_path",
            "created_at",
            "published_at",
            "metrics_json");
    }

    /**
     * 将模型集合转换为 CSV 行。
     */
    private static List<List<String>> modelRows(Collection<RahaColumnModel> models, String modelSetVersion) {
        List<List<String>> rows = new ArrayList<List<String>>();
        for (RahaColumnModel model : models) {
            rows.add(Arrays.asList(
                modelSetVersion,
                model.getDatasetId(),
                model.getColumnName(),
                model.getModelName(),
                model.getModelVersion(),
                String.valueOf(model.getStatus()),
                String.valueOf(model.getClassifierType()),
                model.getFeatureDictionaryVersion(),
                model.getStrategyPlanVersion(),
                String.valueOf(model.getThreshold()),
                model.getModelPath() == null ? "" : model.getModelPath(),
                String.valueOf(model.getCreatedAt()),
                String.valueOf(model.getPublishedAt()),
                FmdbJsonCodec.write(model.getMetrics())));
        }
        return rows;
    }

    /**
     * 从非零特征编号中提取命中的策略标识。
     */
    private static List<String> strategyIds(FeatureDictionary dictionary, SparseFeatureRow row) {
        Set<String> ids = new LinkedHashSet<String>();
        for (Integer index : row.getValues().keySet()) {
            FeatureDefinition definition = dictionary.getDefinitions().get(index);
            if (definition != null && definition.getSource().matches("[0-9a-f]{64}")) {
                ids.add(definition.getSource());
            }
        }
        return Collections.unmodifiableList(new ArrayList<String>(ids));
    }

    /**
     * 写入 UTF-8 JSON 文件。
     */
    private static void writeJson(Path path, Object value) throws IOException {
        writeText(path, FmdbJsonCodec.write(value));
    }

    /**
     * 写入 UTF-8 文本文件。
     */
    private static void writeText(Path path, String value) throws IOException {
        Files.createDirectories(path.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write(value == null ? "" : value);
            writer.newLine();
        }
    }

    /**
     * 写入 UTF-8 CSV 文件。
     */
    private static void writeCsv(Path path, List<String> headers, List<List<String>> rows) throws IOException {
        Files.createDirectories(path.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writeCsvLine(writer, headers);
            for (List<String> row : rows) {
                writeCsvLine(writer, row);
            }
        }
    }

    /**
     * 写入一行 CSV，并处理逗号、引号和换行转义。
     */
    private static void writeCsvLine(BufferedWriter writer, List<String> columns) throws IOException {
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                writer.write(',');
            }
            writer.write(escapeCsv(columns.get(i)));
        }
        writer.newLine();
    }

    /**
     * 转义单个 CSV 单元格。
     */
    private static String escapeCsv(String value) {
        String text = value == null ? "" : value;
        if (text.indexOf(',') >= 0 || text.indexOf('"') >= 0 || text.indexOf('\n') >= 0 || text.indexOf('\r') >= 0) {
            return '"' + text.replace("\"", "\"\"") + '"';
        }
        return text;
    }

    /**
     * 将集合按分隔符拼接。
     */
    private static String join(Collection<?> values, String delimiter) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (Object value : values) {
            if (builder.length() > 0) {
                builder.append(delimiter);
            }
            builder.append(value == null ? "" : String.valueOf(value));
        }
        return builder.toString();
    }

    /**
     * 构造保持插入顺序的 Map，便于输出 JSON 人工阅读。
     */
    private static Map<String, Object> mapOf(Object... keyValues) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return map;
    }

    /**
     * 转换为 Spark 可识别的本地路径。
     */
    private static String toSparkPath(Path path) {
        return path.toAbsolutePath().normalize().toUri().toString();
    }

    /**
     * 将表名转换为适合本地目录的名称。
     */
    private static String safePathName(String value) {
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    /**
     * 将任意文本转换为 Spark 表名可用的标识符片段。
     */
    private static String safeIdentifier(String value) {
        return value.replaceAll("[^A-Za-z0-9_]", "_");
    }

    /**
     * 训练步骤返回值，保留任务元数据用于后续模型发布。
     */
    private static final class TrainingStepResult {

        /** 训练任务执行结果，包含配置版本和快照标识。 */
        private final RahaTaskExecutionResult taskResult;

        /** 训练服务输出，包含模型集合版本和候选模型。 */
        private final RahaTrainOutput output;

        private TrainingStepResult(RahaTaskExecutionResult taskResult, RahaTrainOutput output) {
            this.taskResult = taskResult;
            this.output = output;
        }
    }

    /**
     * 运行参数。
     */
    private static final class AppOptions {

        /** dirty.csv 路径，表示待检测的原始数据。 */
        private final Path dirtyCsvPath;

        /** clean.csv 路径，表示人工标注后的正确数据。 */
        private final Path cleanCsvPath;

        /** 输出根目录，实际结果会落到该目录下的 runId 子目录。 */
        private final Path outputBaseDirectory;

        /** 当前运行编号，用于隔离输出目录、模型和 Spark 临时表。 */
        private final String runId;

        /** dirty.csv 导入后的 Spark 表名。 */
        private final String sourceTableName;

        /** 采样预算，toy 默认覆盖全部七行。 */
        private final int labelingBudget;

        /** Spark master 参数，未指定时沿用提交环境默认配置。 */
        private final String sparkMaster;

        /** 是否在 main 结束时停止 Spark 会话。 */
        private final boolean stopSparkSession;

        /** 是否在启动时删除并重建 Raha 标准 FMDB 物理表，仅用于本地核验。 */
        private final boolean resetFmdbTables;

        private AppOptions(
            Path dirtyCsvPath,
            Path cleanCsvPath,
            Path outputBaseDirectory,
            String runId,
            String sourceTableName,
            int labelingBudget,
            String sparkMaster,
            boolean stopSparkSession,
            boolean resetFmdbTables) {

            this.dirtyCsvPath = dirtyCsvPath.toAbsolutePath().normalize();
            this.cleanCsvPath = cleanCsvPath.toAbsolutePath().normalize();
            this.outputBaseDirectory = outputBaseDirectory.toAbsolutePath().normalize();
            this.runId = runId;
            this.sourceTableName = sourceTableName;
            this.labelingBudget = labelingBudget;
            this.sparkMaster = sparkMaster;
            this.stopSparkSession = stopSparkSession;
            this.resetFmdbTables = resetFmdbTables;
        }

        /**
         * 解析命令行参数，支持 --key=value 格式。
         */
        private static AppOptions parse(String[] args) {
            Map<String, String> arguments = parseArguments(args);
            Path projectRoot = Paths.get(arguments.containsKey("project-root")
                ? arguments.get("project-root")
                : "").toAbsolutePath().normalize();
            String runId = arguments.containsKey("run-id")
                ? safeIdentifier(arguments.get("run-id"))
                : RUN_ID_FORMATTER.format(LocalDateTime.now(ZoneId.systemDefault()));
            Path dirty = resolvePath(projectRoot, arguments.get("dirty"), "datasets/toy/dirty.csv");
            Path clean = resolvePath(projectRoot, arguments.get("clean"), "datasets/toy/clean.csv");
            Path output = resolvePath(projectRoot, arguments.get("output"), "datasets/toy/raha-app-output");
            String sourceTableName = arguments.containsKey("table")
                ? arguments.get("table")
                : "dw.raha_toy_dirty_" + runId;
            int labelingBudget = arguments.containsKey("labeling-budget")
                ? Integer.parseInt(arguments.get("labeling-budget"))
                : 7;
            boolean stopSpark = !Boolean.parseBoolean(arguments.get("keep-spark"));
            boolean resetFmdbTables = Boolean.parseBoolean(arguments.get("reset-fmdb-tables"));
            validateSourceTableName(sourceTableName);
            return new AppOptions(
                dirty,
                clean,
                output,
                runId,
                sourceTableName,
                labelingBudget,
                arguments.get("master"),
                stopSpark,
                resetFmdbTables);
        }

        /**
         * 解析命令行键值参数。
         */
        private static Map<String, String> parseArguments(String[] args) {
            Map<String, String> arguments = new LinkedHashMap<String, String>();
            for (String arg : args) {
                if (arg == null || !arg.startsWith("--")) {
                    throw new IllegalArgumentException("参数必须使用 --key=value 格式: " + arg);
                }
                int separator = arg.indexOf('=');
                if (separator <= 2) {
                    throw new IllegalArgumentException("参数必须使用 --key=value 格式: " + arg);
                }
                String key = arg.substring(2, separator);
                String value = arg.substring(separator + 1);
                arguments.put(key, value);
            }
            return arguments;
        }

        /**
         * 解析路径参数；相对路径按工程根目录解析。
         */
        private static Path resolvePath(Path projectRoot, String value, String defaultValue) {
            Path path = Paths.get(value == null || value.trim().isEmpty() ? defaultValue : value);
            if (!path.isAbsolute()) {
                path = projectRoot.resolve(path);
            }
            return path.toAbsolutePath().normalize();
        }

        /**
         * 防止调用方把 dirty.csv 写入 Raha 标准物理表。
         */
        private static void validateSourceTableName(String sourceTableName) {
            String normalized = sourceTableName.toLowerCase(Locale.ROOT);
            for (FmdbPhysicalTable physicalTable : FmdbPhysicalTable.values()) {
                if (physicalTable.getTableName().equals(normalized)) {
                    throw new IllegalArgumentException("输入数据表不能使用 Raha 标准物理表名: " + sourceTableName);
                }
            }
        }
    }

    /**
     * 当前运行的输出目录集合。
     */
    private static final class RunPaths {

        /** 本次运行的根目录。 */
        private final Path runDirectory;

        /** 输入文件和输入表导出目录。 */
        private final Path inputDirectory;

        /** 采样结果目录。 */
        private final Path samplingDirectory;

        /** 标注模板、自动标注文件和导入结果目录。 */
        private final Path annotationDirectory;

        /** 训练结果摘要目录。 */
        private final Path trainingDirectory;

        /** 模型文件和模型清单目录。 */
        private final Path modelDirectory;

        /** 预测结果目录。 */
        private final Path detectionDirectory;

        /** FMDB 物理表 CSV 导出目录。 */
        private final Path fmdbTablesDirectory;

        /** 采样模板文件。 */
        private final Path annotationTemplateFile;

        /** 根据 clean.csv 填充后的标注模板文件。 */
        private final Path annotationFilledTemplateFile;

        /** 标注导入校验失败时输出的校验文件。 */
        private final Path annotationValidationFile;

        private RunPaths(Path runDirectory) {
            this.runDirectory = runDirectory;
            this.inputDirectory = runDirectory.resolve("input");
            this.samplingDirectory = runDirectory.resolve("sampling");
            this.annotationDirectory = runDirectory.resolve("annotation");
            this.trainingDirectory = runDirectory.resolve("training");
            this.modelDirectory = runDirectory.resolve("model");
            this.detectionDirectory = runDirectory.resolve("detection");
            this.fmdbTablesDirectory = runDirectory.resolve("fmdb-tables");
            this.annotationTemplateFile = annotationDirectory.resolve("template.xls");
            this.annotationFilledTemplateFile = annotationDirectory.resolve("filled-template.xls");
            this.annotationValidationFile = annotationDirectory.resolve("import-validation.xls");
        }

        /**
         * 创建路径对象。
         */
        private static RunPaths create(Path runDirectory) {
            return new RunPaths(runDirectory.toAbsolutePath().normalize());
        }

        /**
         * 创建全部步骤目录。
         */
        private void createDirectories() throws IOException {
            Files.createDirectories(inputDirectory);
            Files.createDirectories(samplingDirectory);
            Files.createDirectories(annotationDirectory);
            Files.createDirectories(trainingDirectory);
            Files.createDirectories(modelDirectory);
            Files.createDirectories(detectionDirectory);
            Files.createDirectories(fmdbTablesDirectory);
        }
    }
}
