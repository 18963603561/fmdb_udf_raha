package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.feature.FeatureDefinition;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.model.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.ColumnPrediction;
import com.fiberhome.ml.raha.model.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.repository.DetectionResultRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public RahaDetectService(PublishedColumnModelLoader modelLoader,
                             ColumnModelPredictor predictor,
                             DetectionResultRepository repository,
                             Clock clock) {
        if (modelLoader == null || predictor == null || repository == null || clock == null) {
            throw new IllegalArgumentException("检测服务依赖不能为空");
        }
        this.modelLoader = modelLoader;
        this.predictor = predictor;
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 对所有有特征字典的字段执行已发布模型检测，单字段失败不丢失其他字段结果。
     *
     * @param request 检测服务输入
     * @return 统一检测任务结果
     */
    public RahaTaskResult<RahaDetectOutput> detect(RahaDetectRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("检测服务请求不能为空");
        }
        long startedAt = clock.millis();
        LOGGER.info("开始 Raha 已发布模型检测，jobId={}，datasetId={}，columnCount={}",
                request.getJobId(), request.getDataset().getDatasetId(),
                request.getFeatures().getDictionaries().size());
        List<DetectionResult> results = new ArrayList<DetectionResult>();
        Map<String, String> modelVersions = new LinkedHashMap<String, String>();
        Map<String, String> failedColumns = new LinkedHashMap<String, String>();
        try {
            for (Map.Entry<String, FeatureDictionary> entry
                    : request.getFeatures().getDictionaries().entrySet()) {
                String columnName = entry.getKey();
                try {
                    ColumnModelArtifact model = modelLoader.load(
                            request.getDataset().getDatasetId(), columnName,
                            request.getDataset().getSchemaHash(), entry.getValue().getVersion(),
                            request.getStrategyPlanVersion());
                    List<SparseFeatureRow> rows = request.getFeatures()
                            .getRowsByColumn(columnName);
                    List<ColumnPrediction> predictions = predictor.predict(model, rows);
                    for (int index = 0; index < rows.size(); index++) {
                        results.add(toDetectionResult(request, entry.getValue(), rows.get(index),
                                predictions.get(index), model, clock.millis()));
                    }
                    modelVersions.put(columnName, model.getModelVersion());
                } catch (RuntimeException exception) {
                    // 字段模型缺失或不兼容时隔离当前字段，并记录上下文和异常堆栈。
                    failedColumns.put(columnName, exception.getClass().getSimpleName());
                    LOGGER.error("字段已发布模型检测失败，jobId={}，columnName={}",
                            request.getJobId(), columnName, exception);
                }
            }
            if (!results.isEmpty()) {
                repository.saveAll(request.getJobId(), results,
                        request.getArtifactVersion(), clock.millis());
            }
            RahaDetectOutput output = new RahaDetectOutput(
                    results, modelVersions, failedColumns);
            RahaTaskStatus status;
            String errorCode = null;
            String errorMessage = null;
            if (modelVersions.isEmpty()) {
                status = RahaTaskStatus.FAILED;
                errorCode = "NO_PUBLISHED_MODEL_RESULT";
                errorMessage = "没有字段完成已发布模型检测";
            } else if (!failedColumns.isEmpty()) {
                status = RahaTaskStatus.PARTIAL_SUCCESS;
                errorCode = "PARTIAL_DETECTION_FAILURE";
                errorMessage = "部分字段没有可用的兼容已发布模型";
            } else {
                status = RahaTaskStatus.SUCCEEDED;
            }
            Map<String, String> details = new LinkedHashMap<String, String>();
            details.put("detectedCellCount", String.valueOf(results.size()));
            details.put("modelVersions", modelVersions.toString());
            RahaTaskSummary summary = new RahaTaskSummary(startedAt, clock.millis(),
                    request.getFeatures().getDictionaries().size(), modelVersions.size(),
                    0L, failedColumns.size(), details);
            LOGGER.info("Raha 已发布模型检测完成，jobId={}，status={}，"
                            + "detectedCellCount={}，failedColumnCount={}",
                    request.getJobId(), status, results.size(), failedColumns.size());
            return new RahaTaskResult<RahaDetectOutput>(request.getJobId(),
                    RahaTaskType.DETECT, status,
                    "repository://detection-result/" + request.getJobId(), summary,
                    output, errorCode, errorMessage);
        } catch (RuntimeException exception) {
            // 检测结果事务保存等任务级异常必须转换为统一失败结果。
            LOGGER.error("Raha 已发布模型检测失败，jobId={}，datasetId={}",
                    request.getJobId(), request.getDataset().getDatasetId(), exception);
            RahaTaskSummary summary = new RahaTaskSummary(startedAt, clock.millis(),
                    1L, 0L, 0L, 1L, Collections.<String, String>emptyMap());
            return new RahaTaskResult<RahaDetectOutput>(request.getJobId(),
                    RahaTaskType.DETECT, RahaTaskStatus.FAILED, null, summary, null,
                    "DETECT_SERVICE_FAILED", exception.getClass().getSimpleName());
        }
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
}
