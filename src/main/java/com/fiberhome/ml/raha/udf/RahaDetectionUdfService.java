package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.annotation.auto.AutoAnnotationConfig;
import com.fiberhome.ml.raha.annotation.auto.AutoAnnotationRequest;
import com.fiberhome.ml.raha.annotation.auto.AutoAnnotationResult;
import com.fiberhome.ml.raha.annotation.auto.AutoAnnotationStatus;
import com.fiberhome.ml.raha.annotation.auto.LlmAutoAnnotationService;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRecord;
import com.fiberhome.ml.raha.annotation.excel.AnnotationExcelConfig;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import com.fiberhome.ml.raha.annotation.service.AnnotationImportRequest;
import com.fiberhome.ml.raha.annotation.service.AnnotationImportResult;
import com.fiberhome.ml.raha.annotation.service.AnnotationImportService;
import com.fiberhome.ml.raha.annotation.service.AnnotationLabelExpander;
import com.fiberhome.ml.raha.annotation.service.AnnotationTemplateRequest;
import com.fiberhome.ml.raha.annotation.service.AnnotationTemplateService;
import com.fiberhome.ml.raha.annotation.service.AnnotationUploadFileLocator;
import com.fiberhome.ml.raha.annotation.service.LocatedAnnotationFile;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingResult;
import com.fiberhome.ml.raha.output.publish.RahaPublishConfig;
import com.fiberhome.ml.raha.output.publish.RahaXlsReportWriter;
import com.fiberhome.ml.raha.output.publish.RahaZipEntrySource;
import com.fiberhome.ml.raha.output.publish.RahaZipWebPublisher;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.task.DetectionRequestOptions;
import com.fiberhome.ml.raha.service.task.FmdbInputSpec;
import com.fiberhome.ml.raha.service.task.FmdbSqlSourceTableResolver;
import com.fiberhome.ml.raha.service.task.MissingModelPolicy;
import com.fiberhome.ml.raha.service.task.RahaTaskApplicationService;
import com.fiberhome.ml.raha.service.task.RahaTaskApplicationServiceFactory;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionRequest;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionResult;
import com.fiberhome.ml.raha.service.task.RahaTaskRequestFactory;
import com.fiberhome.ml.raha.service.task.SamplingRequestOptions;
import com.fiberhome.ml.raha.service.task.TrainingRequestOptions;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchOptions;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import com.fiberhome.ml.raha.util.ReadableIdUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 三个检测 UDF 的应用服务门面，负责连接 GenericUDF 与 Raha 任务工作流。
 */
public final class RahaDetectionUdfService {

    /** 默认标注上传目录。 */
    private static final String DEFAULT_ANNOTATION_DIR =
            "/fmdb/detection/annotation/";
    /** 默认模型根目录。 */
    private static final String DEFAULT_MODEL_BASE_PATH =
            "/fmdb/detection/model/";
    /** 文件名时间格式。 */
    private static final DateTimeFormatter FILE_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
                    .withZone(ZoneId.systemDefault());
    /** 日志记录器。 */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RahaDetectionUdfService.class);

    /** Spark 会话。 */
    private final SparkSession spark;
    /** Raha 统一任务服务。 */
    private final RahaTaskApplicationService taskService;
    /** Raha 最小请求工厂。 */
    private final RahaTaskRequestFactory requestFactory;
    /** 采样记录仓储。 */
    private final SampleRecordRepository sampleRepository;
    /** 标注记录仓储。 */
    private final AnnotationRecordRepository annotationRepository;
    /** 标注模板导出服务。 */
    private final AnnotationTemplateService annotationTemplateService;
    /** 大模型自动标注编排服务。 */
    private final LlmAutoAnnotationService autoAnnotationService;
    /** 标注 Excel 导入服务。 */
    private final AnnotationImportService annotationImportService;
    /** 标注上传文件定位器。 */
    private final AnnotationUploadFileLocator annotationFileLocator;
    /** ZIP 发布器。 */
    private final RahaZipWebPublisher zipPublisher;
    /** Excel 报告写入器。 */
    private final RahaXlsReportWriter xlsReportWriter;
    /** 当前服务使用的时钟。 */
    private final Clock clock;

    private RahaDetectionUdfService(SparkSession spark,
                                    RahaTaskApplicationService taskService,
                                    RahaTaskRequestFactory requestFactory,
                                    Clock clock) {
        if (spark == null || taskService == null || requestFactory == null
                || clock == null) {
            throw new IllegalArgumentException("Raha UDF 服务依赖不能为空");
        }
        this.spark = spark;
        this.taskService = taskService;
        this.requestFactory = requestFactory;
        this.sampleRepository = requestFactory.getSampleRepository();
        this.annotationRepository = requestFactory.getAnnotationRepository();
        AnnotationWorkbookAdapter workbookAdapter =
                new AnnotationWorkbookAdapter(AnnotationExcelConfig.defaults());
        this.annotationTemplateService = new AnnotationTemplateService(
                sampleRepository, workbookAdapter, clock);
        this.annotationImportService = new AnnotationImportService(
                sampleRepository, annotationRepository, workbookAdapter,
                new AnnotationLabelExpander(), clock);
        this.annotationFileLocator = new AnnotationUploadFileLocator();
        this.zipPublisher = new RahaZipWebPublisher();
        this.xlsReportWriter = new RahaXlsReportWriter();
        this.clock = clock;
        this.autoAnnotationService = new LlmAutoAnnotationService(
                workbookAdapter, clock);
    }

    public static RahaDetectionUdfService create(SparkSession spark) {
        Path modelBasePath = Paths.get(configValue("raha.model.base-path",
                DEFAULT_MODEL_BASE_PATH));
        RahaTaskApplicationService taskService =
                RahaTaskApplicationServiceFactory.createDefault(
                        spark, modelBasePath);
        return new RahaDetectionUdfService(spark, taskService,
                taskService.getRequestFactory(), Clock.systemUTC());
    }

    public RahaUdfRows collect(String argument) {
        RahaUdfRequestParser parser = RahaUdfRequestParser.parse(argument);
        FmdbInputSpec input = parser.inputSpec(true);
        LOGGER.info("开始执行采集 UDF，datasetId={}，sourceType={}，requestId={}",
                input.getDatasetId(), parser.sourceType(), parser.optional("requestId"));
        RahaPublishConfig publishConfig = RahaPublishConfig.from(parser.values());
        Path workDir = runWorkDir(publishConfig, "collect");
        RahaTaskExecutionRequest taskRequest = requestFactory.sampling(input,
                new SamplingRequestOptions(parser.intOptional("labelingBudget"),
                        parser.intValue("samplingRound", 1),
                        Collections.emptyList(),
                        parser.executionOverrideOptions()));
        RahaTaskExecutionResult taskResult = taskService.execute(taskRequest);
        RahaSampleOutput output = taskResult.getPayload(RahaSampleOutput.class);
        if (output == null && taskResult.isReused()) {
            return RahaUdfRows.single(summaryOnlyRow(parser, input, taskResult));
        }
        output = requirePayload(taskResult, RahaSampleOutput.class,
                "SAMPLE_OUTPUT_NOT_FOUND", "采集任务没有返回采样输出");
        SampleBatch sampleBatch = output.getSampleBatch();
        if (sampleBatch == null) {
            throw new RahaUdfException("SAMPLE_NOT_FOUND",
                    "采集任务未返回可用于标注的 sampleBatch");
        }
        DatasetSnapshot snapshot = snapshot(taskResult);
        String fileTime = fileTime();
        String excelName = AnnotationUploadFileLocator.annotationExcelName(
                sampleBatch.getSampleBatchId(), fileTime);
        Path excelPath = workDir.resolve(excelName);
        annotationTemplateService.exportTemplate(new AnnotationTemplateRequest(
                sampleBatch.getDatasetId(), sampleBatch.getPartitionMonth(),
                sampleBatch.getSampleBatchId(), sampleBatch.getSnapshotId(),
                excelPath));

        AutoAnnotationConfig autoConfig = autoAnnotationConfig(parser);
        AutoAnnotationResult autoResult = AutoAnnotationResult.disabled();
        if (autoConfig.isEnabled()) {
            String autoExcelName = excelName.substring(0,
                    excelName.length() - ".xls".length()) + "_auto.xls";
            AutoAnnotationRequest autoRequest = new AutoAnnotationRequest(
                    excelPath, workDir.resolve(autoExcelName),
                    workDir.resolve("auto-label"), sampleBatch.getDatasetId(),
                    sampleBatch.getSampleBatchId(), input.getSensitiveColumns());
            try {
                // 外部模型调用失败是否影响采集由 autoLabelFailPolicy 明确控制。
                autoResult = autoAnnotationService.autoLabel(autoRequest,
                        autoConfig);
            } catch (RuntimeException exception) {
                LOGGER.error("采集自动标注失败，sampleBatchId={}，failPolicy={}",
                        sampleBatch.getSampleBatchId(), autoConfig.getFailPolicy(),
                        exception);
                throw new RahaUdfException("AUTO_ANNOTATION_FAILED",
                        "自动标注失败：" + safeExceptionMessage(exception),
                        exception);
            }
        }

        Map<String, Object> row = successBase(parser, input, snapshot);
        enrichExecutionMetadata(row, taskResult);
        row.put("sourceVersion", firstNonBlank(sampleBatch.getSourceVersion(),
                snapshot == null ? null : snapshot.getSourceVersion()));
        row.put("schemaHash", schemaHash(sampleBatch, snapshot));
        row.put("rowCount", snapshot == null
                ? output.getSampling().getMetrics().getCandidateTupleCount()
                : snapshot.getRowCount());
        row.put("fieldCount", Integer.valueOf(fieldCount(sampleBatch, snapshot)));
        row.put("validFieldCount", Integer.valueOf(validFieldCount(
                sampleBatch, snapshot)));
        row.put("sampleBatchId", sampleBatch.getSampleBatchId());
        row.put("sampleRecordCount",
                Long.valueOf(sampleBatch.getRecords().size()));
        row.put("annotationTaskCount",
                Long.valueOf(output.getSampling().getTasks().size()));
        row.put("clusterCount", Integer.valueOf(clusterCount(output)));
        row.put("clusteredFieldCount",
                Integer.valueOf(clusteredFieldCount(output)));
        row.put("annotationExcelName", excelName);
        row.put("partitionMonth", sampleBatch.getPartitionMonth());
        row.putAll(autoResult.toUdfFields());

        String zipName = null;
        String zipUrl = null;
        if (parser.bool("publishZip", true)) {
            zipName = AnnotationUploadFileLocator.collectZipName(
                    sampleBatch.getSampleBatchId(), fileTime);
            List<RahaZipEntrySource> files = collectPackageFiles(workDir,
                    excelPath, autoResult, row, sampleBatch, output);
            try {
                zipUrl = zipPublisher.publish(spark.sqlContext(), files,
                        workDir, zipName, publishConfig);
            } catch (IOException exception) {
                throw new RahaUdfException("PUBLISH_FAILED",
                        "采集 ZIP 发布失败", exception);
            }
        }
        row.put("annotationZipName", zipName);
        row.put("annotationZipUrl", zipUrl);
        return RahaUdfRows.single(row);
    }

    public RahaUdfRows train(String argument) {
        RahaUdfRequestParser parser = RahaUdfRequestParser.parse(argument);
        String sampleBatchId = parser.required("sampleBatchId", "采样批次标识");
        LOGGER.info("开始执行训练 UDF，sampleBatchId={}，requestId={}",
                sampleBatchId, parser.optional("requestId"));
        Optional<SampleBatch> sampleOptional =
                sampleRepository.findByBatchId(sampleBatchId);
        if (!sampleOptional.isPresent()) {
            return RahaUdfRows.single(failedBase("SAMPLE_NOT_FOUND",
                    "sampleBatchId 不存在：" + sampleBatchId,
                    parser, null, sampleBatchId));
        }
        SampleBatch sampleBatch = sampleOptional.get();
        boolean allowPartial = parser.bool("allowPartialAnnotation", false);
        RahaPublishConfig publishConfig = RahaPublishConfig.from(parser.values());
        Path workDir = runWorkDir(publishConfig, "train");
        String annotationDir = parser.optional("annotationDir",
                configValue("raha.annotation.upload-dir",
                        DEFAULT_ANNOTATION_DIR));
        Optional<LocatedAnnotationFile> located = annotationFileLocator
                .findLatest(spark, annotationDir, sampleBatchId,
                        workDir.resolve("annotation-upload"));
        AnnotationBatch annotationBatch;
        String annotationFileName = null;
        if (located.isPresent()) {
            LocatedAnnotationFile file = located.get();
            annotationFileName = file.getFileName();
            annotationBatch = importAnnotationFile(parser, sampleBatch,
                    allowPartial, file, workDir);
        } else {
            Optional<AnnotationBatch> latest = annotationRepository
                    .findLatestTrainableForSample(sampleBatchId, allowPartial);
            if (!latest.isPresent()) {
                return RahaUdfRows.single(failedBase(
                        "MANUAL_ANNOTATION_NOT_FOUND",
                        "未找到人工标注数据，请将标注 Excel 上传到 HDFS 路径 "
                                + DEFAULT_ANNOTATION_DIR + " 下后重新训练",
                        parser, null, sampleBatchId));
            }
            annotationBatch = latest.get();
            annotationFileName = annotationFileName(annotationBatch);
        }

        String requestedSnapshotId = parser.optional("snapshotId");
        ColumnBatchOptions columnBatchOptions = columnBatchOptions(parser);
        boolean reuseSnapshotCheckpoint = requestedSnapshotId != null
                && trainingInputMissing(parser);
        if (reuseSnapshotCheckpoint && columnBatchOptions.isEnabled()) {
            // 列批训练由检查点仓储按字段裁剪恢复，避免每个子任务加载全字段产物。
            LOGGER.info("列批训练启用快照检查点按字段恢复，sampleBatchId={}，snapshotId={}",
                    sampleBatchId, requestedSnapshotId);
        }
        if (reuseSnapshotCheckpoint
                && !requestedSnapshotId.equals(sampleBatch.getSnapshotId())) {
            return RahaUdfRows.single(failedBase("SNAPSHOT_MISMATCH",
                    "训练复用的 snapshotId 必须与采样批次一致",
                    parser, null, sampleBatchId));
        }
        FmdbInputSpec inputOverride = reuseSnapshotCheckpoint
                ? null : parser.inputSpec(false);
        TrainingRequestOptions options = new TrainingRequestOptions(
                allowPartial, parser.optional("modelNamePrefix", "raha"),
                LabelPropagationMethod.HOMOGENEITY, inputOverride,
                parser.executionOverrideOptions(), requestedSnapshotId,
                reuseSnapshotCheckpoint, columnBatchOptions);
        RahaTaskExecutionRequest taskRequest = requestFactory.training(
                Collections.singletonList(sampleBatchId), options);
        RahaTaskExecutionResult taskResult = taskService.execute(taskRequest);
        RahaTrainOutput output = taskResult.getPayload(RahaTrainOutput.class);
        if (output == null && taskResult.isReused()) {
            return RahaUdfRows.single(summaryOnlyRow(parser, null, taskResult));
        }
        output = requirePayload(taskResult, RahaTrainOutput.class,
                "TRAIN_OUTPUT_NOT_FOUND", "训练任务没有返回模型输出");
        DatasetSnapshot snapshot = snapshot(taskResult);
        List<Map<String, Object>> rows = trainRows(parser, sampleBatch,
                annotationBatch, annotationFileName, output, snapshot,
                taskResult);
        if (parser.bool("publishZip", true)) {
            publishTrainReport(parser, publishConfig, workDir, rows, output,
                    inputOverride, snapshot, sampleBatch);
        }
        return new RahaUdfRows(rows);
    }

    public RahaUdfRows detect(String argument) {
        RahaUdfRequestParser parser = RahaUdfRequestParser.parse(argument);
        FmdbInputSpec input = parser.inputSpec(true);
        String modelSetVersion = parser.optional("modelSetVersion");
        MissingModelPolicy policy = missingModelPolicy(parser);
        LOGGER.info("开始执行检测 UDF，datasetId={}，modelSetVersion={}，missingModelPolicy={}",
                input.getDatasetId(), modelSetVersion, policy);
        RahaTaskExecutionRequest taskRequest = requestFactory.detection(
                input, modelSetVersion, new DetectionRequestOptions(policy,
                        parser.executionOverrideOptions(),
                        columnBatchOptions(parser)));
        String resolvedModelSetVersion = taskRequest.getModelSetVersion();
        RahaTaskExecutionResult taskResult = taskService.execute(taskRequest);
        RahaDetectOutput output = taskResult.getPayload(RahaDetectOutput.class);
        if (output == null && taskResult.isReused()) {
            return RahaUdfRows.single(summaryOnlyRow(parser, input, taskResult));
        }
        output = requirePayload(taskResult, RahaDetectOutput.class,
                "DETECT_OUTPUT_NOT_FOUND", "检测任务没有返回检测输出");
        DatasetSnapshot snapshot = snapshot(taskResult);
        RahaDataset dataset = dataset(taskResult);
        Map<String, Object> row = successBase(parser, input, snapshot);
        enrichExecutionMetadata(row, taskResult);
        row.put("modelSetVersion", resolvedModelSetVersion);
        row.put("schemaHash", snapshot == null ? null : snapshot.getSchemaHash());
        row.put("rowCount", snapshot == null ? null
                : Long.valueOf(snapshot.getRowCount()));
        row.put("fieldCount", snapshot == null ? null
                : Integer.valueOf(snapshot.getColumnCount()));
        row.put("validFieldCount", Integer.valueOf(validFieldCount(dataset)));
        Object targetColumnCount = taskResult.getResultSummary().get(
                "targetColumnCount");
        if (targetColumnCount instanceof Number) {
            row.put("validFieldCount", Integer.valueOf(
                    ((Number) targetColumnCount).intValue()));
        }
        row.put("modelFieldCount",
                Integer.valueOf(output.getModelVersions().size()));
        row.put("failedFieldCount",
                Integer.valueOf(output.getFailedColumns().size()));
        row.put("detectedCellCount",
                Long.valueOf(output.getResults().size()));
        row.put("detectedErrorCount",
                Long.valueOf(errorCount(output.getResults())));
        row.put("resultTable", FmdbPhysicalTable.DETECTION_RESULT.getTableName());

        if (parser.bool("publishZip", true)) {
            RahaPublishConfig publishConfig = RahaPublishConfig.from(parser.values());
            Path workDir = runWorkDir(publishConfig, "detect");
            String fileTime = fileTime();
            String sourceToken = outputSourceToken(input, snapshot, dataset,
                    null, resolvedModelSetVersion);
            String detailName = "raha-detrun-detail_" + sourceToken
                    + "_" + fileTime + ".xls";
            String zipName = "raha-detrun_" + sourceToken
                    + "_" + fileTime + ".zip";
            List<Map<String, Object>> detailRows =
                    detectionDetailRows(taskResult, output);
            Path detailPath = xlsReportWriter.write(workDir.resolve(detailName),
                    "detection_detail", detectionDetailHeaders(), detailRows);
            Path summaryPath = writeJson(workDir.resolve("summary.json"), row);
            try {
                String url = zipPublisher.publish(spark.sqlContext(),
                        Arrays.asList(
                                new RahaZipEntrySource(detailPath,
                                        "detail/" + detailName),
                                new RahaZipEntrySource(summaryPath,
                                        "summary.json")),
                        workDir, zipName, publishConfig);
                row.put("detailZipName", zipName);
                row.put("detailZipUrl", url);
            } catch (IOException exception) {
                throw new RahaUdfException("PUBLISH_FAILED",
                        "检测明细 ZIP 发布失败", exception);
            }
        }
        return RahaUdfRows.single(row);
    }

    private AnnotationBatch importAnnotationFile(RahaUdfRequestParser parser,
                                                 SampleBatch sampleBatch,
                                                 boolean allowPartial,
                                                 LocatedAnnotationFile file,
                                                 Path workDir) {
        try {
            AnnotationImportResult result = annotationImportService.importWorkbook(
                    new AnnotationImportRequest(sampleBatch.getDatasetId(),
                            sampleBatch.getPartitionMonth(),
                            sampleBatch.getSampleBatchId(), file.getLocalPath(),
                            workDir.resolve("validation_"
                                    + file.getFileName()),
                            parser.caller(), false, null, null));
            if (result.getStatus() == AnnotationBatchStatus.DUPLICATE) {
                Optional<AnnotationBatch> latest = annotationRepository
                        .findLatestTrainableForSample(
                                sampleBatch.getSampleBatchId(), allowPartial);
                if (latest.isPresent()) {
                    return latest.get();
                }
                throw new RahaUdfException("ANNOTATION_IMPORT_FAILED",
                        "标注文件重复但未找到可复用的已导入标注结果");
            }
            if (result.getStatus() == AnnotationBatchStatus.REJECTED) {
                throw new RahaUdfException("ANNOTATION_IMPORT_FAILED",
                        "标注 Excel 没有有效标注行，错误数量："
                                + result.getErrors().size());
            }
            if (result.getStatus() == AnnotationBatchStatus.PARTIAL
                    && !allowPartial) {
                throw new RahaUdfException("ANNOTATION_IMPORT_FAILED",
                        "标注 Excel 仅部分导入，当前 allowPartialAnnotation=false");
            }
            return result.getBatch();
        } catch (RahaUdfException exception) {
            throw exception;
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("ANNOTATION_FILE_INVALID",
                    "标注 Excel 无法读取或模板结构非法：" + exception.getMessage(),
                    exception);
        } catch (RuntimeException exception) {
            throw new RahaUdfException("ANNOTATION_IMPORT_FAILED",
                    "标注 Excel 导入失败：" + exception.getMessage(),
                    exception);
        }
    }

    private List<Map<String, Object>> trainRows(RahaUdfRequestParser parser,
                                                SampleBatch sampleBatch,
                                                 AnnotationBatch annotationBatch,
                                                 String annotationFileName,
                                                 RahaTrainOutput output,
                                                 DatasetSnapshot snapshot,
                                                 RahaTaskExecutionResult taskResult) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        Set<String> columns = new LinkedHashSet<String>();
        columns.addAll(output.getTrainingResults().keySet());
        columns.addAll(output.getCandidateModels().keySet());
        if (columns.isEmpty()) {
            columns.add("");
        }
        for (String column : columns) {
            Map<String, Object> row = successBase(parser, null, snapshot);
            enrichExecutionMetadata(row, taskResult);
            row.put("datasetId", sampleBatch.getDatasetId());
            row.put("sampleBatchId", sampleBatch.getSampleBatchId());
            row.put("annotationBatchId",
                    annotationBatch.getAnnotationBatchId());
            row.put("annotationFileName", annotationFileName);
            row.put("annotationStatus", annotationBatch.getStatus().name());
            AnnotationRecord first = firstAnnotationRecord(annotationBatch);
            row.put("annotationRecordCount", first == null ? null
                    : Long.valueOf(first.getBatchRecordCount()));
            row.put("validAnnotationCount", first == null ? null
                    : Long.valueOf(first.getValidRecordCount()));
            row.put("invalidAnnotationCount", first == null ? null
                    : Long.valueOf(first.getInvalidRecordCount()));
            row.put("modelSetVersion", output.getModelSetVersion());
            row.put("columnName", column);
            ColumnModelTrainingResult result =
                    output.getTrainingResults().get(column);
            RahaColumnModel model = output.getCandidateModels().get(column);
            if (model != null) {
                row.put("modelVersion", model.getModelVersion());
                row.put("modelStatus", model.getStatus().name());
                row.put("classifierType", model.getClassifierType().name());
                row.put("featureDictionaryVersion",
                        model.getFeatureDictionaryVersion());
                row.put("strategyPlanVersion",
                        model.getStrategyPlanVersion());
                row.put("threshold", Double.valueOf(model.getThreshold()));
                row.put("metricJson", FmdbJsonCodec.write(model.getMetrics()));
            } else if (result != null) {
                row.put("modelStatus", result.getStatus().name());
                row.put("errorCode", result.getStatus().name());
                row.put("errorMessage", result.getMessage());
                row.put("metricJson", FmdbJsonCodec.write(result.getMetrics()));
            }
            rows.add(row);
        }
        return rows;
    }

    private void publishTrainReport(RahaUdfRequestParser parser,
                                    RahaPublishConfig publishConfig,
                                     Path workDir,
                                     List<Map<String, Object>> rows,
                                     RahaTrainOutput output,
                                     FmdbInputSpec input,
                                     DatasetSnapshot snapshot,
                                     SampleBatch sampleBatch) {
        String version = output.getModelSetVersion() == null
                ? "unknown" : output.getModelSetVersion();
        String fileTime = fileTime();
        String sourceToken = outputSourceToken(input, snapshot, null,
                sampleBatch, version);
        String reportName = "raha-dettrain_" + sourceToken
                + "_" + fileTime + ".xls";
        String zipName = "raha-dettrain_" + sourceToken
                + "_" + fileTime + ".zip";
        Path reportPath = xlsReportWriter.write(workDir.resolve(reportName),
                "train_report", trainReportHeaders(), rows);
        Path summaryPath = writeJson(workDir.resolve("summary.json"),
                trainSummary(output));
        try {
            String url = zipPublisher.publish(spark.sqlContext(),
                    Arrays.asList(
                            new RahaZipEntrySource(reportPath,
                                    "report/" + reportName),
                            new RahaZipEntrySource(summaryPath,
                                    "summary.json")),
                    workDir, zipName, publishConfig);
            for (Map<String, Object> row : rows) {
                row.put("reportZipName", zipName);
                row.put("reportZipUrl", url);
            }
        } catch (IOException exception) {
            throw new RahaUdfException("PUBLISH_FAILED",
                    "训练报告 ZIP 发布失败", exception);
        }
    }

    private List<RahaZipEntrySource> collectPackageFiles(Path workDir,
                                                         Path excelPath,
                                                         AutoAnnotationResult autoResult,
                                                         Map<String, Object> row,
                                                         SampleBatch sampleBatch,
                                                         RahaSampleOutput output) {
        Path summary = writeJson(workDir.resolve("summary.json"), row);
        Path manifest = writeJson(workDir.resolve("manifest.json"),
                collectManifest(sampleBatch));
        Path clusters = writeText(workDir.resolve("column-clusters.csv"),
                clusterCsv(output));
        List<RahaZipEntrySource> files = new ArrayList<RahaZipEntrySource>();
        files.add(new RahaZipEntrySource(excelPath,
                "annotation/" + excelPath.getFileName()));
        if (autoResult != null
                && autoResult.getStatus()
                != AutoAnnotationStatus.DISABLED) {
            files.add(new RahaZipEntrySource(excelPath,
                    "annotation/raw/" + excelPath.getFileName()));
            if (autoResult.getWorkbookPath() != null) {
                files.add(new RahaZipEntrySource(autoResult.getWorkbookPath(),
                        "annotation/auto/"
                                + autoResult.getWorkbookPath().getFileName()));
            }
            if (autoResult.getSummaryPath() != null) {
                files.add(new RahaZipEntrySource(autoResult.getSummaryPath(),
                        "auto-label/summary.json"));
            }
            if (autoResult.getDecisionsPath() != null) {
                files.add(new RahaZipEntrySource(autoResult.getDecisionsPath(),
                        "auto-label/decisions.jsonl"));
            }
            if (autoResult.getBatchesPath() != null) {
                files.add(new RahaZipEntrySource(autoResult.getBatchesPath(),
                        "auto-label/batches.jsonl"));
            }
        }
        files.add(new RahaZipEntrySource(summary, "summary.json"));
        files.add(new RahaZipEntrySource(manifest, "manifest.json"));
        files.add(new RahaZipEntrySource(clusters, "column-clusters.csv"));
        return files;
    }

    private List<Map<String, Object>> detectionDetailRows(
            RahaTaskExecutionResult taskResult,
            RahaDetectOutput output) {
        List<Map<String, Object>> persisted =
                persistedDetectionDetails(taskResult.getJob().getJobId());
        if (!persisted.isEmpty()) {
            return persisted;
        }
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (DetectionResult result : output.getResults()) {
            if (!result.isError()) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("dataset_id", result.getCoordinate().getDatasetId());
            row.put("snapshot_id", result.getCoordinate().getSnapshotId());
            row.put("row_id", result.getCoordinate().getRowId());
            row.put("column_name", result.getCoordinate().getColumnName());
            // DetectionResult 仍沿用旧字段名，UDF 明细按新契约输出 original_value。
            row.put("original_value", result.getMaskedValue());
            row.put("score", Double.valueOf(result.getScore()));
            row.put("threshold", Double.valueOf(result.getThreshold()));
            row.put("detected_as_error", Boolean.valueOf(result.isError()));
            row.put("model_name", result.getModelName());
            row.put("model_version", result.getModelVersion());
            row.put("feature_dictionary_version",
                    result.getFeatureDictionaryVersion());
            row.put("strategy_ids", result.getStrategyIds().toString());
            row.put("reasons_json", FmdbJsonCodec.write(result.getReasons()));
            rows.add(row);
        }
        return rows;
    }

    private List<Map<String, Object>> persistedDetectionDetails(String jobId) {
        try {
            Dataset<Row> dataset = spark.table(
                    FmdbPhysicalTable.DETECTION_RESULT.getTableName())
                    .where(functions.col("detection_batch_id").equalTo(jobId));
            List<Row> sparkRows = dataset.collectAsList();
            List<Map<String, Object>> rows =
                    new ArrayList<Map<String, Object>>(sparkRows.size());
            for (Row sparkRow : sparkRows) {
                Map<String, Object> reason = FmdbJsonCodec.readObject(
                        (String) sparkRow.getAs("error_reason_json"));
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                row.put("dataset_id", sparkRow.getAs("dataset_id"));
                row.put("snapshot_id", reason.get("snapshotId"));
                row.put("row_id", sparkRow.getAs("row_id"));
                row.put("column_name", sparkRow.getAs("column_name"));
                row.put("original_value", sparkRow.getAs("original_value"));
                row.put("score", sparkRow.getAs("score"));
                row.put("threshold", sparkRow.getAs("threshold"));
                row.put("detected_as_error", Boolean.TRUE);
                row.put("model_name", reason.get("modelName"));
                row.put("model_version", sparkRow.getAs("model_version"));
                row.put("feature_dictionary_version",
                        reason.get("featureDictionaryVersion"));
                row.put("strategy_ids", String.valueOf(reason.get("strategyIds")));
                row.put("reasons_json", FmdbJsonCodec.write(reason.get("reasons")));
                rows.add(row);
            }
            return rows;
        } catch (Exception exception) {
            LOGGER.warn("读取 FMDB 检测结果明细失败，使用内存结果兜底，jobId={}",
                    jobId, exception);
            return Collections.emptyList();
        }
    }

    private Map<String, Object> successBase(RahaUdfRequestParser parser,
                                            FmdbInputSpec input,
                                            DatasetSnapshot snapshot) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("status", "SUCCESS");
        row.put("datasetId", snapshot == null
                ? input == null ? null : input.getDatasetId()
                : snapshot.getDatasetId());
        row.put("snapshotId", snapshot == null
                ? input == null ? null : input.getSnapshotId()
                : snapshot.getSnapshotId());
        row.put("sourceType", input == null ? parser.sourceType()
                : sourceType(input));
        row.put("inputReference", input == null ? null
                : parser.inputReferenceSummary(input));
        row.put("createdAt", Long.valueOf(clock.millis()));
        return row;
    }

    private Map<String, Object> failedBase(String code,
                                           String message,
                                           RahaUdfRequestParser parser,
                                           FmdbInputSpec input,
                                           String sampleBatchId) {
        Map<String, Object> row = successBase(parser, input, null);
        row.put("status", "FAILED");
        row.put("errorCode", code);
        row.put("errorMessage", message);
        row.put("sampleBatchId", sampleBatchId);
        return row;
    }

    private Map<String, Object> summaryOnlyRow(RahaUdfRequestParser parser,
                                               FmdbInputSpec input,
                                               RahaTaskExecutionResult result) {
        Map<String, Object> row = successBase(parser, input, null);
        row.putAll(result.getResultSummary());
        row.put("status", udfStatus(result.getJob().getStatus()));
        row.put("errorCode", result.getJob().getErrorCode());
        row.put("errorMessage", result.getJob().getErrorMessage());
        enrichExecutionMetadata(row, result);
        return row;
    }

    private void enrichExecutionMetadata(Map<String, Object> row,
                                         RahaTaskExecutionResult result) {
        row.put("jobId", result.getJob().getJobId());
        row.put("configVersion", result.getJob().getConfigVersion());
        row.put("idempotentKey", result.getJob().getIdempotentKey());
        row.put("reused", Boolean.valueOf(result.isReused()));
        copyExecutionValue(row, result, "forceRun");
        copyExecutionValue(row, result, "forceRunId");
        copyExecutionValue(row, result, "baseExecutionInputFingerprint");
        copyExecutionValue(row, result, "executionInputFingerprint");
        copyExecutionValue(row, result, "currentSelectRule");
    }

    private static void copyExecutionValue(Map<String, Object> row,
                                           RahaTaskExecutionResult result,
                                           String key) {
        Object value = result.getResultSummary().get(key);
        if (value == null) {
            value = result.getAttributes().get(key);
        }
        row.put(key, value);
    }

    private static String udfStatus(JobStatus status) {
        if (status == JobStatus.SUCCEEDED
                || status == JobStatus.PARTIAL_SUCCESS) {
            return "SUCCESS";
        }
        if (status == JobStatus.FAILED || status == JobStatus.CANCELLED) {
            return "FAILED";
        }
        return status.name();
    }

    private DatasetSnapshot snapshot(RahaTaskExecutionResult result) {
        Object value = result.getAttributes().get(
                StageAttributeKeys.DATASET_SNAPSHOT);
        return value instanceof DatasetSnapshot ? (DatasetSnapshot) value : null;
    }

    private RahaDataset dataset(RahaTaskExecutionResult result) {
        Object value = result.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        return value instanceof RahaDataset ? (RahaDataset) value : null;
    }

    private <T> T requirePayload(RahaTaskExecutionResult result,
                                 Class<T> type,
                                 String errorCode,
                                 String message) {
        T payload = result.getPayload(type);
        if (payload == null) {
            throw new RahaUdfException(errorCode,
                    result.isReused() ? message + "，幂等任务复用结果缺少 payload"
                            : message);
        }
        return payload;
    }

    private Path runWorkDir(RahaPublishConfig config, String type) {
        Path path = config.getWorkDir().resolve(type)
                .resolve(fileTime() + "-" + Math.abs(clock.millis()));
        try {
            Files.createDirectories(path);
            return path;
        } catch (IOException exception) {
            throw new RahaUdfException("PUBLISH_FAILED",
                    "创建 UDF 工作目录失败", exception);
        }
    }

    private Path writeJson(Path path, Object value) {
        return writeText(path, Collections.singletonList(FmdbJsonCodec.write(value)));
    }

    private Path writeText(Path path, List<String> lines) {
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            Files.write(path, lines, StandardCharsets.UTF_8);
            return path;
        } catch (IOException exception) {
            throw new RahaUdfException("PUBLISH_FAILED",
                    "写入 UDF 产物文件失败：" + path, exception);
        }
    }

    private Map<String, Object> collectManifest(SampleBatch sampleBatch) {
        Map<String, Object> manifest = new LinkedHashMap<String, Object>();
        manifest.put("sampleBatchId", sampleBatch.getSampleBatchId());
        manifest.put("datasetId", sampleBatch.getDatasetId());
        manifest.put("samplePartitionMonth", sampleBatch.getPartitionMonth());
        manifest.put("snapshotId", sampleBatch.getSnapshotId());
        manifest.put("samplingVersion", sampleBatch.getSamplingVersion());
        return manifest;
    }

    private Map<String, Object> trainSummary(RahaTrainOutput output) {
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("modelSetVersion", output.getModelSetVersion());
        summary.put("strategyPlanVersion", output.getStrategyPlanVersion());
        summary.put("trainedColumnCount", output.getCandidateModels().size());
        summary.put("trainingResultCount", output.getTrainingResults().size());
        return summary;
    }

    private List<String> clusterCsv(RahaSampleOutput output) {
        List<String> lines = new ArrayList<String>();
        lines.add("columnName,status,effectiveClusterCount,clusterVersion");
        for (ColumnClusteringResult result
                : output.getClustering().getResults().values()) {
            lines.add(csv(result.getColumnName()) + ","
                    + csv(result.getStatus().name()) + ","
                    + result.getEffectiveClusterCount() + ","
                    + csv(result.getClusterVersion()));
        }
        return lines;
    }

    private static List<String> trainReportHeaders() {
        return Arrays.asList("status", "errorCode", "errorMessage",
                "datasetId", "snapshotId", "sampleBatchId",
                "annotationBatchId", "annotationFileName", "annotationStatus",
                "annotationRecordCount", "validAnnotationCount",
                "invalidAnnotationCount", "modelSetVersion", "columnName",
                "modelVersion", "modelStatus", "classifierType",
                "featureDictionaryVersion", "strategyPlanVersion", "threshold",
                "metricJson");
    }

    private static List<String> detectionDetailHeaders() {
        return Arrays.asList("dataset_id", "snapshot_id", "row_id",
                "column_name", "original_value", "score", "threshold",
                "detected_as_error", "model_name", "model_version",
                "feature_dictionary_version", "strategy_ids", "reasons_json");
    }

    private static int clusterCount(RahaSampleOutput output) {
        int count = 0;
        for (ColumnClusteringResult result
                : output.getClustering().getResults().values()) {
            count += result.getEffectiveClusterCount();
        }
        return count;
    }

    private static int clusteredFieldCount(RahaSampleOutput output) {
        int count = 0;
        for (ColumnClusteringResult result
                : output.getClustering().getResults().values()) {
            if (result.getEffectiveClusterCount() > 0) {
                count++;
            }
        }
        return count;
    }

    private static int fieldCount(SampleBatch sampleBatch,
                                  DatasetSnapshot snapshot) {
        if (snapshot != null) {
            return snapshot.getColumnCount();
        }
        List<Map<String, Object>> columns = columnDefinitions(sampleBatch);
        return columns.isEmpty() ? firstRecord(sampleBatch).getRowData().size()
                : columns.size();
    }

    private static int validFieldCount(SampleBatch sampleBatch,
                                       DatasetSnapshot snapshot) {
        List<Map<String, Object>> columns = columnDefinitions(sampleBatch);
        if (columns.isEmpty()) {
            return snapshot == null ? 0 : snapshot.getColumnCount();
        }
        int count = 0;
        for (Map<String, Object> column : columns) {
            if (Boolean.TRUE.equals(column.get("detectable"))) {
                count++;
            }
        }
        return count;
    }

    private static int validFieldCount(RahaDataset dataset) {
        if (dataset == null) {
            return 0;
        }
        int count = 0;
        for (ColumnMetadata column : dataset.getColumns()) {
            if (column.isDetectable()) {
                count++;
            }
        }
        return count;
    }

    private static String schemaHash(SampleBatch sampleBatch,
                                     DatasetSnapshot snapshot) {
        if (snapshot != null) {
            return snapshot.getSchemaHash();
        }
        return firstRecord(sampleBatch).getSchemaHash();
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> columnDefinitions(
            SampleBatch sampleBatch) {
        Object columns = firstRecord(sampleBatch).getColumnSchema()
                .get("columns");
        if (!(columns instanceof List)) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> result =
                new ArrayList<Map<String, Object>>();
        for (Object column : (List<Object>) columns) {
            if (column instanceof Map) {
                result.add((Map<String, Object>) column);
            }
        }
        return result;
    }

    private static SampleRecord firstRecord(SampleBatch sampleBatch) {
        return sampleBatch.getRecords().get(0);
    }

    private static AnnotationRecord firstAnnotationRecord(
            AnnotationBatch annotationBatch) {
        return annotationBatch.getRecords().isEmpty()
                ? null : annotationBatch.getRecords().get(0);
    }

    private static String annotationFileName(AnnotationBatch batch) {
        AnnotationRecord first = firstAnnotationRecord(batch);
        return first == null ? null : first.getFileName();
    }

    private static long errorCount(List<DetectionResult> results) {
        long count = 0L;
        for (DetectionResult result : results) {
            if (result.isError()) {
                count++;
            }
        }
        return count;
    }

    private static boolean trainingInputMissing(RahaUdfRequestParser parser) {
        return parser.optional("sourceType") == null
                && parser.optional("datasetId") == null
                && parser.optional("sqlText") == null
                && parser.optional("sql") == null
                && parser.optional("tableName") == null
                && parser.optional("table") == null;
    }

    private static String sourceType(FmdbInputSpec input) {
        return input.getFormat() == DataFormat.FMDB_SQL ? "SQL" : "TABLE";
    }

    private static MissingModelPolicy missingModelPolicy(
            RahaUdfRequestParser parser) {
        String value = parser.optional("missingModelPolicy", "PARTIAL")
                .toUpperCase(Locale.ROOT);
        try {
            return MissingModelPolicy.valueOf(value);
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_ARGUMENT",
                    "missingModelPolicy 只支持 FAIL 或 PARTIAL", exception);
        }
    }

    private String fileTime() {
        return FILE_TIME_FORMAT.format(Instant.ofEpochMilli(clock.millis()));
    }

    private static String outputSourceToken(FmdbInputSpec input,
                                            DatasetSnapshot snapshot,
                                            RahaDataset dataset,
                                            SampleBatch sampleBatch,
                                            String fallback) {
        // 发布文件名优先使用库表名，避免采样批次、模型版本里的哈希泄漏到用户可见文件。
        String sourceName = snapshot == null ? null : snapshot.getTableName();
        if (isBlank(sourceName) && dataset != null) {
            sourceName = dataset.getTableName();
        }
        if (isBlank(sourceName) && input != null) {
            sourceName = input.getTableName();
        }
        if (isBlank(sourceName)) {
            sourceName = sourceNameFromSample(sampleBatch);
        }
        if (isBlank(sourceName)) {
            sourceName = sourceNameFromVersion(fallback);
        }
        return sourceFileToken(sourceName);
    }

    private static String sourceNameFromSample(SampleBatch sampleBatch) {
        if (sampleBatch == null || sampleBatch.getRecords().isEmpty()) {
            return sampleBatch == null ? null : sampleBatch.getDatasetId();
        }
        SampleRecord record = firstRecord(sampleBatch);
        // 兼容旧采样批次：优先使用已持久化的输入来源，旧 SQL 哈希引用再尝试从上下文 SQL 解析。
        String sourceName = sourceNameFromReference(record.getInputReference());
        if (!isBlank(sourceName)) {
            return sourceName;
        }
        Map<String, Object> context = record.getSamplingContext();
        Object readReference = context.get(
                SampleRecord.READ_INPUT_REFERENCE_CONTEXT_KEY);
        Object sourceType = context.get(SampleRecord.SOURCE_TYPE_CONTEXT_KEY);
        String type = sourceType == null ? "" : String.valueOf(sourceType);
        sourceName = sourceNameFromReadReference(
                readReference == null ? null : String.valueOf(readReference),
                type);
        return isBlank(sourceName) ? sampleBatch.getDatasetId() : sourceName;
    }

    private static String sourceNameFromReadReference(String reference,
                                                      String sourceType) {
        if (isBlank(reference)) {
            return null;
        }
        String type = sourceType == null ? "" : sourceType.trim()
                .toUpperCase(Locale.ROOT);
        if ("SQL".equals(type)) {
            // SQL 来源按首个真实表命名，保证文件名落到库名和表名。
            return sourceNameFromSql(reference);
        }
        return sourceNameFromReference(reference);
    }

    private static String sourceNameFromReference(String reference) {
        if (isBlank(reference)) {
            return null;
        }
        String text = reference.trim();
        String lower = text.toLowerCase(Locale.ROOT);
        if (lower.startsWith("select ") || lower.startsWith("with ")) {
            return sourceNameFromSql(text);
        }
        if (lower.startsWith("sql:")) {
            return null;
        }
        return text;
    }

    private static String sourceNameFromSql(String sqlText) {
        try {
            return FmdbSqlSourceTableResolver.firstSourceTable(sqlText);
        } catch (RuntimeException exception) {
            LOGGER.debug("解析 SQL 来源表失败，降级使用后续命名来源，sqlLength={}",
                    sqlText == null ? 0 : sqlText.length(), exception);
            return null;
        }
    }

    private static String sourceNameFromVersion(String version) {
        if (isBlank(version)) {
            return null;
        }
        String text = version.trim();
        int at = text.indexOf('@');
        return at > 0 ? text.substring(0, at) : text;
    }

    private static String sourceFileToken(String sourceName) {
        if (isBlank(sourceName)) {
            return "unknown";
        }
        try {
            return safeFileToken(ReadableIdUtils.normalizeSourceName(sourceName));
        } catch (IllegalArgumentException exception) {
            LOGGER.debug("规范化来源名失败，降级使用安全文件片段，sourceName={}",
                    sourceName, exception);
            return safeFileToken(sourceName);
        }
    }

    private static String safeFileToken(String value) {
        String text = value == null || value.trim().isEmpty()
                ? "unknown" : value.trim();
        text = text.replaceAll("[\\\\/:*?\"<>|\\s]+", "_");
        text = text.replaceAll("_+", "_");
        text = text.replaceAll("[^a-zA-Z0-9._\\-]", "_");
        return text.isEmpty() ? "unknown" : text;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String firstNonBlank(String first, String second) {
        return first == null || first.trim().isEmpty() ? second : first;
    }

    private static String csv(String value) {
        String text = value == null ? "" : value;
        if (text.contains(",") || text.contains("\"")
                || text.contains("\n")) {
            return "\"" + text.replace("\"", "\"\"") + "\"";
        }
        return text;
    }

    private static String configValue(String key, String defaultValue) {
        String value = RahaDefaultConfigProvider.properties().asMap().get(key);
        return value == null || value.trim().isEmpty()
                ? defaultValue : value.trim();
    }

    /**
     * 从 UDF 参数和默认配置创建列批执行参数。
     */
    private static ColumnBatchOptions columnBatchOptions(
            RahaUdfRequestParser parser) {
        int batchSize = parser.intValue("columnBatchSize",
                configInt("raha.column-batch.size", 10));
        int maxParallel = parser.intValue("maxParallelColumnBatches",
                configInt("raha.column-batch.max-parallel", 1));
        boolean rvdEnabled = parser.bool("batchRvdEnabled",
                configBoolean("raha.column-batch.rvd.enabled", false));
        boolean failFast = parser.bool("failFastColumnBatch",
                configBoolean("raha.column-batch.fail-fast", false));
        return new ColumnBatchOptions(batchSize, maxParallel,
                rvdEnabled, failFast);
    }

    private static int configInt(String key, int defaultValue) {
        String value = configValue(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new RahaUdfException("INVALID_CONFIGURATION",
                    key + " 必须为整数", exception);
        }
    }

    private static boolean configBoolean(String key, boolean defaultValue) {
        String value = configValue(key, String.valueOf(defaultValue));
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new RahaUdfException("INVALID_CONFIGURATION",
                key + " 必须为 true 或 false");
    }

    /**
     * 解析自动标注参数并统一转换为 UDF 参数错误。
     */
    private static AutoAnnotationConfig autoAnnotationConfig(
            RahaUdfRequestParser parser) {
        try {
            return AutoAnnotationConfig.from(parser.values());
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_ARGUMENT",
                    exception.getMessage(), exception);
        }
    }

    /**
     * 生成不含多行内容的安全异常摘要，避免外部响应进入 UDF 返回值。
     */
    private static String safeExceptionMessage(Throwable exception) {
        String message = exception.getMessage();
        if (message == null || message.trim().isEmpty()) {
            return exception.getClass().getSimpleName();
        }
        String compact = message.replaceAll("[\\r\\n\\t]+", " ").trim();
        return compact.length() <= 500 ? compact : compact.substring(0, 500);
    }
}
