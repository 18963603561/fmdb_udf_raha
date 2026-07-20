package com.fiberhome.ml.raha.service.detect;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.prediction.ColumnPrediction;
import com.fiberhome.ml.raha.model.release.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.parallel.ParallelBatchResult;
import com.fiberhome.ml.raha.parallel.ParallelFailure;
import com.fiberhome.ml.raha.parallel.ParallelWorkItem;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultSaveContext;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.service.common.RahaServiceSummary;
import com.fiberhome.ml.raha.data.type.JobType;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按字段加载唯一已发布模型，执行兼容校验和批量检测并保存结果。
 */
public final class RahaDetectService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaDetectService.class);
    /** 已发布模型兼容加载器。 */
    private final PublishedColumnModelLoader modelLoader;
    /** 可移植列级模型预测器。 */
    private final ColumnModelPredictor predictor;
    /** 检测结果仓储。 */
    private final DetectionResultRepository repository;
    /** 提供可测试检测时间的时钟。 */
    private final Clock clock;
    /** 受限列预测并行执行器。 */
    private final BoundedParallelExecutor parallelExecutor;

    public RahaDetectService(PublishedColumnModelLoader modelLoader,
                             ColumnModelPredictor predictor,
                             DetectionResultRepository repository,
                             Clock clock) {
        this(modelLoader, predictor, repository, clock,
                new BoundedParallelExecutor());
    }

    public RahaDetectService(PublishedColumnModelLoader modelLoader,
                             ColumnModelPredictor predictor,
                             DetectionResultRepository repository,
                             Clock clock,
                             BoundedParallelExecutor parallelExecutor) {
        if (modelLoader == null || predictor == null || repository == null || clock == null) {
            throw new IllegalArgumentException("检测服务依赖不能为空");
        }
        if (parallelExecutor == null) {
            throw new IllegalArgumentException("检测并行执行器不能为空");
        }
        this.modelLoader = modelLoader;
        this.predictor = predictor;
        this.repository = repository;
        this.clock = clock;
        this.parallelExecutor = parallelExecutor;
    }

    /**
     * 对所有有特征字典的字段执行已发布模型检测，单字段失败不丢失其他字段结果。
     *
     * @param request 检测服务输入
     * @return 统一检测任务结果
     */
    public RahaServiceResult<RahaDetectOutput> detect(RahaDetectRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("检测服务请求不能为空");
        }
        long startedAt = clock.millis();
        LOGGER.info("开始 Raha 已发布模型检测，jobId={}，datasetId={}，"
                        + "modelSetVersion={}，columnCount={}",
                request.getJobId(), request.getDataset().getDatasetId(),
                request.getModelSetVersion(),
                request.getFeatures().getDictionaries().size());
        List<DetectionResult> results = new ArrayList<DetectionResult>();
        Map<String, String> modelVersions = new LinkedHashMap<String, String>();
        Map<String, String> failedColumns = new LinkedHashMap<String, String>();
        try {
            List<ParallelWorkItem<String, DetectColumnOutcome>> items =
                    new ArrayList<ParallelWorkItem<String, DetectColumnOutcome>>();
            for (Map.Entry<String, FeatureDictionary> entry
                    : request.getFeatures().getDictionaries().entrySet()) {
                items.add(new ParallelWorkItem<String, DetectColumnOutcome>(
                        entry.getKey(), () -> detectColumn(
                        request, entry.getKey(), entry.getValue())));
            }
            ParallelBatchResult<String, DetectColumnOutcome> parallel =
                    parallelExecutor.execute(items,
                            request.getResourceConfig().getMaxParallelColumns(),
                            request.getResourceConfig().getStageTimeoutMillis());
            for (String columnName : request.getFeatures().getDictionaries().keySet()) {
                DetectColumnOutcome outcome = parallel.getSuccesses().get(columnName);
                if (outcome != null) {
                    results.addAll(outcome.results);
                    modelVersions.put(columnName, outcome.modelVersion);
                    continue;
                }
                ParallelFailure failure = parallel.getFailures().get(columnName);
                failedColumns.put(columnName, failure == null
                        ? "RuntimeException" : failure.getErrorType());
            }
            RahaDetectOutput output = new RahaDetectOutput(
                    results, modelVersions, failedColumns);
            JobStatus status;
            String errorCode = null;
            String errorMessage = null;
            if (modelVersions.isEmpty()) {
                status = JobStatus.FAILED;
                errorCode = "NO_PUBLISHED_MODEL_RESULT";
                errorMessage = "没有字段完成已发布模型检测";
            } else if (!failedColumns.isEmpty()
                    && request.getMissingModelPolicy()
                    == com.fiberhome.ml.raha.service.task.MissingModelPolicy.FAIL) {
                status = JobStatus.FAILED;
                errorCode = "MODEL_SET_INCOMPLETE";
                errorMessage = "指定模型集合存在缺失或不兼容字段";
            } else if (!failedColumns.isEmpty()) {
                status = JobStatus.PARTIAL_SUCCESS;
                errorCode = "PARTIAL_DETECTION_FAILURE";
                errorMessage = "部分字段没有可用的兼容已发布模型";
            } else {
                status = JobStatus.SUCCEEDED;
            }
            long errorCellCount = errorCellCount(results);
            long writtenErrorCount = 0L;
            if (!results.isEmpty() && status != JobStatus.FAILED) {
                writtenErrorCount = repository.saveAll(new DetectionResultSaveContext(
                                request.getJobId(), request.getDataset(),
                                request.getModelSetVersion(),
                                modelVersions.values()), results,
                        request.getArtifactVersion(), clock.millis());
            }
            Map<String, String> details = new LinkedHashMap<String, String>();
            details.put("detectedCellCount", String.valueOf(results.size()));
            details.put("errorCellCount", String.valueOf(errorCellCount));
            details.put("writtenErrorCount", String.valueOf(writtenErrorCount));
            details.put("detectionBatchId", request.getJobId());
            details.put("modelVersions", modelVersions.toString());
            details.put("modelSetVersion", String.valueOf(
                    request.getModelSetVersion()));
            details.put("scoreDiagnostics", scoreDiagnostics(results));
            details.put("maxObservedColumnConcurrency",
                    String.valueOf(parallel.getMaxObservedConcurrency()));
            RahaServiceSummary summary = new RahaServiceSummary(startedAt, clock.millis(),
                    request.getFeatures().getDictionaries().size(), modelVersions.size(),
                    0L, failedColumns.size(), details);
            LOGGER.info("Raha 已发布模型检测完成，jobId={}，status={}，"
                            + "detectedCellCount={}，failedColumnCount={}",
                    request.getJobId(), status, results.size(), failedColumns.size());
            return new RahaServiceResult<RahaDetectOutput>(request.getJobId(),
                    JobType.DETECTION, status,
                    "repository://detection-result/" + request.getJobId(), summary,
                    output, errorCode, errorMessage);
        } catch (RuntimeException exception) {
            // 检测结果事务保存等任务级异常必须转换为统一失败结果。
            LOGGER.error("Raha 已发布模型检测失败，jobId={}，datasetId={}",
                    request.getJobId(), request.getDataset().getDatasetId(), exception);
            RahaServiceSummary summary = new RahaServiceSummary(startedAt, clock.millis(),
                    1L, 0L, 0L, 1L, Collections.<String, String>emptyMap());
            return new RahaServiceResult<RahaDetectOutput>(request.getJobId(),
                    JobType.DETECTION, JobStatus.FAILED, null, summary, null,
                    "DETECT_SERVICE_FAILED", exception.getClass().getSimpleName());
        }
    }

    private DetectColumnOutcome detectColumn(RahaDetectRequest request,
                                             String columnName,
                                             FeatureDictionary dictionary) {
        ColumnModelArtifact model = request.getModelSetVersion() == null
                ? modelLoader.load(request.getDataset().getDatasetId(), columnName,
                request.getDataset().getSchemaHash(), dictionary.getVersion(),
                request.getStrategyPlanVersion())
                : modelLoader.load(request.getModelSetVersion(),
                request.getDataset().getDatasetId(), columnName,
                request.getDataset().getSchemaHash(), dictionary.getVersion(),
                request.getStrategyPlanVersion());
        List<SparseFeatureRow> rows = request.getFeatures().getRowsByColumn(columnName);
        List<ColumnPrediction> predictions = predictor.predict(model, rows);
        List<DetectionResult> results = new ArrayList<DetectionResult>(rows.size());
        for (int index = 0; index < rows.size(); index++) {
            results.add(toDetectionResult(request, dictionary, rows.get(index),
                    predictions.get(index), model, clock.millis()));
        }
        return new DetectColumnOutcome(results, model.getModelVersion());
    }

    private static DetectionResult toDetectionResult(RahaDetectRequest request,
                                                      FeatureDictionary dictionary,
                                                      SparseFeatureRow row,
                                                      ColumnPrediction prediction,
                                                      ColumnModelArtifact model,
                                                      long detectedAt) {
        if (row.getCoordinate() == null || row.getValueHash() == null) {
            throw new IllegalArgumentException("生产检测特征必须包含单元格坐标和值哈希");
        }
        Map<String, String> reasons = new LinkedHashMap<String, String>();
        reasons.put("classifierType", model.getClassifierType().name());
        reasons.put("trainingMode", model.getTrainingMode());
        return new DetectionResult(request.getJobId(), request.getConfigVersion(),
                request.getStageId(), row.getCoordinate(), row.getValueHash(),
                row.getMaskedValue(), prediction.isError(), prediction.getScore(),
                prediction.getThreshold(), strategyIds(dictionary, row), reasons,
                model.getModelName(), model.getModelVersion(),
                model.getFeatureDictionaryVersion(), detectedAt);
    }

    private static List<String> strategyIds(FeatureDictionary dictionary,
                                            SparseFeatureRow row) {
        Set<String> ids = new LinkedHashSet<String>();
        for (Integer index : row.getValues().keySet()) {
            FeatureDefinition definition = dictionary.getDefinitions().get(index);
            if (definition != null && definition.getSource().matches("[0-9a-f]{64}")) {
                ids.add(definition.getSource());
            }
        }
        return Collections.unmodifiableList(new ArrayList<String>(ids));
    }

    private static String scoreDiagnostics(List<DetectionResult> results) {
        Map<String, ScoreAccumulator> byColumn =
                new LinkedHashMap<String, ScoreAccumulator>();
        for (DetectionResult result : results) {
            String columnName = result.getCoordinate().getColumnName();
            if (!byColumn.containsKey(columnName)) {
                byColumn.put(columnName, new ScoreAccumulator());
            }
            byColumn.get(columnName).add(result.getScore(), result.isError());
        }
        Map<String, String> summaries = new LinkedHashMap<String, String>();
        for (Map.Entry<String, ScoreAccumulator> entry : byColumn.entrySet()) {
            summaries.put(entry.getKey(), entry.getValue().summary());
        }
        return summaries.toString();
    }

    private static long errorCellCount(List<DetectionResult> results) {
        long count = 0L;
        for (DetectionResult result : results) {
            if (result.isError()) {
                count++;
            }
        }
        return count;
    }

    private static final class DetectColumnOutcome {
        /** 当前字段全部检测结果。 */
        private final List<DetectionResult> results;
        /** 当前字段已发布模型版本。 */
        private final String modelVersion;

        private DetectColumnOutcome(List<DetectionResult> results,
                                    String modelVersion) {
            this.results = results;
            this.modelVersion = modelVersion;
        }
    }

    /** 累计单字段分数范围和预测正例比例。 */
    private static final class ScoreAccumulator {
        /** 预测数量。 */
        private long count;
        /** 预测正例数量。 */
        private long positiveCount;
        /** 最小分数。 */
        private double minimum = Double.POSITIVE_INFINITY;
        /** 最大分数。 */
        private double maximum = Double.NEGATIVE_INFINITY;
        /** 分数总和。 */
        private double sum;

        private void add(double score, boolean positive) {
            count++;
            if (positive) {
                positiveCount++;
            }
            minimum = Math.min(minimum, score);
            maximum = Math.max(maximum, score);
            sum += score;
        }

        private String summary() {
            double mean = count == 0L ? 0.0d : sum / count;
            double positiveRatio = count == 0L ? 0.0d
                    : (double) positiveCount / count;
            return "count=" + count + ",min=" + minimum + ",max=" + maximum
                    + ",mean=" + mean + ",positiveRatio=" + positiveRatio;
        }
    }
}
