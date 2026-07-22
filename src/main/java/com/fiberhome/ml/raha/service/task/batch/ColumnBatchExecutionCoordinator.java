package com.fiberhome.ml.raha.service.task.batch;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.release.ModelReadableVersioner;
import com.fiberhome.ml.raha.model.release.ModelSourceKey;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingResult;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionRequest;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionResult;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 串行执行训练或检测列批子任务，并生成父任务轻量汇总。
 */
public final class ColumnBatchExecutionCoordinator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ColumnBatchExecutionCoordinator.class);
    /** 轻量输入模式解析器。 */
    private final ColumnBatchSchemaResolver schemaResolver;
    /** 稳定列批规划器。 */
    private final ColumnBatchPlanner planner;
    /** 模型版本和耗时使用的统一时钟。 */
    private final Clock clock;

    public ColumnBatchExecutionCoordinator(
            ColumnBatchSchemaResolver schemaResolver,
            ColumnBatchPlanner planner,
            Clock clock) {
        if (schemaResolver == null || planner == null || clock == null) {
            throw new IllegalArgumentException("列批协调器依赖不能为空");
        }
        this.schemaResolver = schemaResolver;
        this.planner = planner;
        this.clock = clock;
    }

    /**
     * 判断请求的目标字段数是否确实需要拆成多个列批。
     *
     * @param request 父任务请求
     * @return 目标字段数大于列批大小时返回真
     */
    public boolean shouldBatch(RahaTaskExecutionRequest request) {
        ColumnBatchOptions options = request.getColumnBatchOptions();
        return options.isEnabled()
                && schemaResolver.resolve(request.getDataLoadRequest()).size()
                > options.getColumnBatchSize();
    }

    /**
     * 使用父任务标识生成稳定列批。
     *
     * @param request 父任务请求
     * @param parentJobId 父任务标识
     * @return 有序列批
     */
    public List<ColumnBatch> plan(RahaTaskExecutionRequest request,
                                  String parentJobId) {
        return planner.plan(parentJobId,
                schemaResolver.resolve(request.getDataLoadRequest()),
                request.getColumnBatchOptions().getColumnBatchSize());
    }

    /**
     * 串行执行全部列批并汇总字段结果。
     *
     * @param parentRequest 父任务请求
     * @param parentJobId 父任务标识
     * @param batches 已规划列批
     * @param childExecutor 单任务执行入口
     * @return 父任务阶段可消费的列批结果
     */
    public ColumnBatchExecutionOutcome execute(
            RahaTaskExecutionRequest parentRequest,
            String parentJobId,
            List<ColumnBatch> batches,
            Function<RahaTaskExecutionRequest, RahaTaskExecutionResult>
                    childExecutor) {
        if (parentRequest == null || batches == null || batches.isEmpty()
                || childExecutor == null) {
            throw new IllegalArgumentException("列批执行参数不能为空");
        }
        long startedAt = clock.millis();
        JobType jobType = parentRequest.getConfig().getJobType();
        List<String> targetColumns = flattenColumns(batches);
        String modelSetVersion = jobType == JobType.TRAINING
                ? modelSetVersion(parentRequest, parentJobId) : null;
        String modelCompatibilityVersion = jobType == JobType.TRAINING
                ? modelCompatibilityVersion(parentRequest, parentJobId,
                targetColumns) : null;
        String detectionBatchId = jobType == JobType.DETECTION
                ? parentJobId : null;
        String globalVersion = jobType == JobType.TRAINING
                ? modelSetVersion : detectionBatchId;
        LOGGER.info("开始执行列批父任务，parentJobId={}，jobType={}，"
                        + "batchCount={}，targetColumnCount={}，globalVersion={}",
                parentJobId, jobType, batches.size(), targetColumns.size(),
                globalVersion);

        List<ColumnBatchTaskResult> taskResults =
                new ArrayList<ColumnBatchTaskResult>();
        List<String> succeededColumns = new ArrayList<String>();
        Map<String, String> failedColumns = new LinkedHashMap<String, String>();
        Map<String, Object> parentAttributes = new LinkedHashMap<String, Object>();
        Map<String, ColumnModelTrainingResult> trainingResults =
                new LinkedHashMap<String, ColumnModelTrainingResult>();
        Map<String, RahaColumnModel> candidateModels =
                new LinkedHashMap<String, RahaColumnModel>();
        List<DetectionResult> detectionResults = new ArrayList<DetectionResult>();
        Map<String, String> modelVersions = new LinkedHashMap<String, String>();
        RahaTrainOutput representativeTrainOutput = null;
        int nextBatchIndex = 0;

        for (ColumnBatch batch : batches) {
            nextBatchIndex = batch.getBatchIndex() + 1;
            long batchStartedAt = clock.millis();
            RahaTaskExecutionResult childResult = null;
            RuntimeException executionFailure = null;
            try {
                RahaTaskExecutionRequest childRequest = childRequest(
                        parentRequest, parentJobId, batch, batches.size(),
                        modelSetVersion, modelCompatibilityVersion,
                        detectionBatchId);
                LOGGER.info("开始执行列批子任务，parentJobId={}，batchId={}，"
                                + "batchIndex={}，columns={}",
                        parentJobId, batch.getBatchId(), batch.getBatchIndex(),
                        batch.getColumns());
                childResult = childExecutor.apply(childRequest);
            } catch (RuntimeException exception) {
                executionFailure = exception;
                LOGGER.error("列批子任务执行异常，parentJobId={}，batchId={}，columns={}",
                        parentJobId, batch.getBatchId(), batch.getColumns(),
                        exception);
            }
            long elapsedMillis = Math.max(0L,
                    clock.millis() - batchStartedAt);
            JobStatus status = childResult == null
                    ? JobStatus.FAILED : childResult.getJob().getStatus();
            String errorCode = executionFailure == null
                    ? childResult == null ? "COLUMN_BATCH_RESULT_REQUIRED"
                    : childResult.getJob().getErrorCode()
                    : executionFailure.getClass().getSimpleName();
            String errorMessage = executionFailure == null
                    ? childResult == null ? "列批子任务没有返回结果"
                    : childResult.getJob().getErrorMessage()
                    : executionFailure.getMessage();
            String childJobId = childResult == null
                    ? null : childResult.getJob().getJobId();
            taskResults.add(new ColumnBatchTaskResult(batch, childJobId,
                    status, elapsedMillis, errorCode, errorMessage));

            if (childResult != null) {
                copyRepresentativeAttributes(parentAttributes,
                        childResult.getAttributes());
                if (jobType == JobType.TRAINING) {
                    RahaTrainOutput output = childResult.getPayload(
                            RahaTrainOutput.class);
                    if (output != null) {
                        trainingResults.putAll(output.getTrainingResults());
                        candidateModels.putAll(output.getCandidateModels());
                        if (representativeTrainOutput == null
                                && !output.getCandidateModels().isEmpty()) {
                            representativeTrainOutput = output;
                        }
                    }
                } else {
                    RahaDetectOutput output = childResult.getPayload(
                            RahaDetectOutput.class);
                    if (output != null) {
                        detectionResults.addAll(output.getResults());
                        modelVersions.putAll(output.getModelVersions());
                        failedColumns.putAll(output.getFailedColumns());
                    }
                }
            }

            if (status == JobStatus.FAILED) {
                addBatchFailure(failedColumns, batch,
                        errorCode == null ? "COLUMN_BATCH_FAILED" : errorCode);
                if (parentRequest.getColumnBatchOptions()
                        .isFailFastColumnBatch()) {
                    LOGGER.warn("列批父任务触发快速失败，parentJobId={}，batchId={}",
                            parentJobId, batch.getBatchId());
                    break;
                }
            }
        }

        if (nextBatchIndex < batches.size()) {
            for (int index = nextBatchIndex; index < batches.size(); index++) {
                ColumnBatch skipped = batches.get(index);
                addBatchFailure(failedColumns, skipped,
                        "SKIPPED_BY_FAIL_FAST");
                taskResults.add(new ColumnBatchTaskResult(skipped, null,
                        JobStatus.FAILED, 0L, "SKIPPED_BY_FAIL_FAST",
                        "前序列批失败，当前批次未执行"));
            }
        }

        if (jobType == JobType.TRAINING) {
            succeededColumns.addAll(candidateModels.keySet());
            for (String column : targetColumns) {
                if (!candidateModels.containsKey(column)
                        && !failedColumns.containsKey(column)) {
                    ColumnModelTrainingResult result = trainingResults.get(column);
                    failedColumns.put(column, result == null
                            ? "NO_TRAINING_RESULT" : result.getStatus().name());
                }
            }
            if (representativeTrainOutput != null) {
                parentAttributes.put(StageAttributeKeys.TRAIN_OUTPUT,
                        RahaTrainOutput.columnBatchSummary(
                                representativeTrainOutput, trainingResults,
                                candidateModels, modelCompatibilityVersion,
                                modelSetVersion));
                parentAttributes.put(StageAttributeKeys.RESULT_LOCATION,
                        "repository://column-model/"
                                + parentRequest.getConfig().getDatasetId());
            }
        } else {
            succeededColumns.addAll(modelVersions.keySet());
            parentAttributes.put(StageAttributeKeys.DETECT_OUTPUT,
                    new RahaDetectOutput(detectionResults, modelVersions,
                            failedColumns));
            parentAttributes.put(StageAttributeKeys.RESULT_LOCATION,
                    "repository://detection-result/" + detectionBatchId);
        }
        ColumnBatchExecutionSummary summary = new ColumnBatchExecutionSummary(
                parentJobId, jobType, parentRequest.getColumnBatchOptions(),
                targetColumns.size(), globalVersion, taskResults,
                succeededColumns, failedColumns,
                Math.max(0L, clock.millis() - startedAt));
        parentAttributes.put(StageAttributeKeys.COLUMN_BATCH_SUMMARY,
                summary.toSummaryMap());
        LOGGER.info("列批父任务执行完成，parentJobId={}，batchCount={}，"
                        + "succeededColumnCount={}，failedColumnCount={}，elapsedMillis={}",
                parentJobId, batches.size(), succeededColumns.size(),
                failedColumns.size(), Math.max(0L, clock.millis() - startedAt));
        return new ColumnBatchExecutionOutcome(summary, parentAttributes);
    }

    private static RahaTaskExecutionRequest childRequest(
            RahaTaskExecutionRequest parent,
            String parentJobId,
            ColumnBatch batch,
            int batchCount,
            String modelSetVersion,
            String modelCompatibilityVersion,
            String detectionBatchId) {
        Set<String> columns = new LinkedHashSet<String>(batch.getColumns());
        StrategyConfig strategyConfig = parent.getConfig().getStrategyConfig()
                .withColumnBatch(columns,
                        parent.getColumnBatchOptions().isBatchRvdEnabled());
        String childFingerprint = HashUtils.md5Hex(
                parent.getExecutionInputFingerprint() + "|" + parentJobId
                        + "|" + batch.getBatchId() + "|" + batch.getColumns());
        RahaJobConfig childConfig = parent.getConfig()
                .withStrategyConfig(strategyConfig)
                .withExecutionInputFingerprint(childFingerprint);
        DataLoadRequest childLoadRequest = parent.getDataLoadRequest()
                .withIncludedColumns(columns);
        return parent.toColumnBatchChild(childConfig, childLoadRequest,
                new ColumnBatchContext(parentJobId, batch, batchCount),
                modelSetVersion, modelCompatibilityVersion, detectionBatchId);
    }

    private String modelSetVersion(RahaTaskExecutionRequest request,
                                   String parentJobId) {
        ModelSourceKey source = ModelSourceKey.fromDatasetAndTable(
                request.getConfig().getDatasetId(),
                request.getDataLoadRequest().getTableName());
        return ModelReadableVersioner.modelSetVersion(source.getSourceName(),
                clock.millis(), parentJobId);
    }

    private static String modelCompatibilityVersion(
            RahaTaskExecutionRequest request,
            String parentJobId,
            List<String> columns) {
        return HashUtils.md5Hex("column-batch-model-plan|" + parentJobId
                + "|" + request.getExecutionInputFingerprint() + "|" + columns);
    }

    private static List<String> flattenColumns(List<ColumnBatch> batches) {
        List<String> result = new ArrayList<String>();
        for (ColumnBatch batch : batches) {
            result.addAll(batch.getColumns());
        }
        return result;
    }

    private static void addBatchFailure(Map<String, String> failedColumns,
                                        ColumnBatch batch,
                                        String errorCode) {
        for (String column : batch.getColumns()) {
            if (!failedColumns.containsKey(column)) {
                failedColumns.put(column, errorCode);
            }
        }
    }

    private static void copyRepresentativeAttributes(
            Map<String, Object> target,
            Map<String, Object> source) {
        copyIfAbsent(target, source, StageAttributeKeys.RAHA_DATASET);
        copyIfAbsent(target, source, StageAttributeKeys.DATASET_SNAPSHOT);
    }

    private static void copyIfAbsent(Map<String, Object> target,
                                     Map<String, Object> source,
                                     String key) {
        if (!target.containsKey(key) && source.containsKey(key)) {
            target.put(key, source.get(key));
        }
    }
}
