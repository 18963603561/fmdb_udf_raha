package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.service.common.RahaServiceSummary;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将任务执行结果压缩为可持久化、可回填的轻量摘要。
 */
final class RahaTaskResultSummaryBuilder {

    private RahaTaskResultSummaryBuilder() {
    }

    /**
     * 根据请求和执行结果创建摘要。
     *
     * @param request 任务请求
     * @param runResult 执行结果
     * @return 可写入 result_summary_json 的有序摘要
     */
    static Map<String, Object> build(RahaTaskExecutionRequest request,
                                     JobRunResult runResult) {
        Map<String, Object> result =
                new LinkedHashMap<String, Object>(
                        request.executionSummarySeed());
        result.put("jobId", runResult.getJob().getJobId());
        result.put("configVersion", runResult.getJob().getConfigVersion());
        result.put("idempotentKey", runResult.getJob().getIdempotentKey());
        result.put("jobStatus", runResult.getJob().getStatus().name());
        result.put("errorCode", runResult.getJob().getErrorCode());
        result.put("errorMessage", runResult.getJob().getErrorMessage());

        Map<String, Object> attributes = runResult.getAttributes();
        String resultLocation = resultLocation(attributes);
        if (resultLocation != null) {
            result.put("resultLocation", resultLocation);
        }
        appendServiceSummary(result, serviceResult(attributes));
        appendSampleSummary(result, payload(attributes,
                StageAttributeKeys.SAMPLE_OUTPUT, RahaSampleOutput.class));
        appendTrainSummary(result, payload(attributes,
                StageAttributeKeys.TRAIN_OUTPUT, RahaTrainOutput.class));
        appendDetectSummary(result, request, runResult, payload(attributes,
                StageAttributeKeys.DETECT_OUTPUT, RahaDetectOutput.class));
        return result;
    }

    private static void appendServiceSummary(Map<String, Object> result,
                                             RahaServiceResult<?> serviceResult) {
        if (serviceResult == null) {
            return;
        }
        result.put("serviceStatus", serviceResult.getStatus().name());
        if (serviceResult.getResultLocation() != null) {
            result.put("resultLocation", serviceResult.getResultLocation());
        }
        RahaServiceSummary summary = serviceResult.getSummary();
        Map<String, Object> serviceSummary = new LinkedHashMap<String, Object>();
        serviceSummary.put("startedAt", Long.valueOf(summary.getStartedAt()));
        serviceSummary.put("completedAt", Long.valueOf(summary.getCompletedAt()));
        serviceSummary.put("elapsedMillis", Long.valueOf(summary.getElapsedMillis()));
        serviceSummary.put("totalCount", Long.valueOf(summary.getTotalCount()));
        serviceSummary.put("successfulCount",
                Long.valueOf(summary.getSuccessfulCount()));
        serviceSummary.put("skippedCount", Long.valueOf(summary.getSkippedCount()));
        serviceSummary.put("failedCount", Long.valueOf(summary.getFailedCount()));
        serviceSummary.put("details", summary.getDetails());
        result.put("serviceSummary", serviceSummary);
    }

    private static void appendSampleSummary(Map<String, Object> result,
                                            RahaSampleOutput output) {
        if (output == null) {
            return;
        }
        SampleBatch sampleBatch = output.getSampleBatch();
        if (sampleBatch != null) {
            result.put("sampleBatchId", sampleBatch.getSampleBatchId());
            result.put("partitionMonth", sampleBatch.getPartitionMonth());
            result.put("sourceVersion", sampleBatch.getSourceVersion());
            result.put("sampleRecordCount",
                    Long.valueOf(sampleBatch.getRecords().size()));
            result.put("currentBatchId", sampleBatch.getSampleBatchId());
        }
        result.put("annotationTaskCount",
                Long.valueOf(output.getSampling().getTasks().size()));
        result.put("candidateTupleCount",
                Long.valueOf(output.getSampling().getMetrics()
                        .getCandidateTupleCount()));
    }

    private static void appendTrainSummary(Map<String, Object> result,
                                           RahaTrainOutput output) {
        if (output == null) {
            return;
        }
        result.put("modelSetVersion", output.getModelSetVersion());
        result.put("strategyPlanVersion", output.getStrategyPlanVersion());
        result.put("trainedColumnCount",
                Integer.valueOf(output.getCandidateModels().size()));
        result.put("trainingResultCount",
                Integer.valueOf(output.getTrainingResults().size()));
        result.put("currentBatchId", output.getModelSetVersion());
    }

    private static void appendDetectSummary(Map<String, Object> result,
                                            RahaTaskExecutionRequest request,
                                            JobRunResult runResult,
                                            RahaDetectOutput output) {
        if (output == null) {
            return;
        }
        if (request.getModelSetVersion() != null) {
            result.put("modelSetVersion", request.getModelSetVersion());
        }
        result.put("currentBatchId", runResult.getJob().getJobId());
        result.put("modelFieldCount",
                Integer.valueOf(output.getModelVersions().size()));
        result.put("failedFieldCount",
                Integer.valueOf(output.getFailedColumns().size()));
        result.put("detectedCellCount",
                Long.valueOf(output.getResults().size()));
        result.put("detectedErrorCount",
                Long.valueOf(errorCount(output.getResults())));
    }

    private static RahaServiceResult<?> serviceResult(
            Map<String, Object> attributes) {
        Object value = attributes.get(StageAttributeKeys.SAMPLE_SERVICE_RESULT);
        if (value == null) {
            value = attributes.get(StageAttributeKeys.TRAIN_SERVICE_RESULT);
        }
        if (value == null) {
            value = attributes.get(StageAttributeKeys.DETECT_SERVICE_RESULT);
        }
        return value instanceof RahaServiceResult
                ? (RahaServiceResult<?>) value : null;
    }

    private static String resultLocation(Map<String, Object> attributes) {
        Object value = attributes.get(StageAttributeKeys.RESULT_LOCATION);
        return value instanceof String ? (String) value : null;
    }

    private static <T> T payload(Map<String, Object> attributes,
                                 String key,
                                 Class<T> payloadType) {
        Object value = attributes.get(key);
        return payloadType.isInstance(value) ? payloadType.cast(value) : null;
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
}
