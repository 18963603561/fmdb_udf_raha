package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskSummary;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.FormDataCodec;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证三个 Raha 表级 UDF 的直接执行、类型隔离和结果契约。
 */
class RahaTableUdfIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldCallDedicatedHandlerAndReturnCompletedResult() {
        TrackingHandlers handlers = new TrackingHandlers();

        String sample = new F_DW_RAHASAMPLE(handlers)
                .call(sampleRequest("sample-job"));
        String train = new F_DW_RAHATRAIN(handlers)
                .call(trainRequest("train-job"));
        String detect = new F_DW_RAHADETECT(handlers)
                .call(detectRequest("detect-job"));

        assertEquals(1, handlers.sampleCount.get());
        assertEquals(1, handlers.trainCount.get());
        assertEquals(1, handlers.detectCount.get());
        assertTrue(sample.contains("\"status\":\"SUCCEEDED\""));
        assertTrue(train.contains("\"taskType\":\"TRAIN\""));
        assertTrue(detect.contains("\"jobId\":\"detect-job\""));
        assertTrue(detect.contains("\"summary\":{"));
        assertFalse(sample.contains("ACCEPTED"));
        assertFalse(sample.contains("DUPLICATE"));
    }

    @Test
    void shouldRejectInvalidAndCrossOperationArgumentsBeforeHandler() {
        TrackingHandlers handlers = new TrackingHandlers();
        Map<String, String> missingModel = commonValues("missing-model");
        Map<String, String> crossTask = trainValues("cross-task");
        crossTask.put("modelVersion", "model-v1");

        String detect = new F_DW_RAHADETECT(handlers)
                .call(FormDataCodec.encode(missingModel));
        String train = new F_DW_RAHATRAIN(handlers)
                .call(FormDataCodec.encode(crossTask));
        String unknown = new F_DW_RAHASAMPLE(handlers)
                .call(sampleRequest("unknown") + "&unexpected=value");
        String malformed = new F_DW_RAHATRAIN(handlers)
                .call("not-a-form-request");

        assertTrue(detect.contains("\"status\":\"REJECTED\""));
        assertTrue(detect.contains("\"errorCode\":\"INVALID_UDF_ARGUMENT\""));
        assertTrue(train.contains("\"errorCode\":\"INVALID_UDF_ARGUMENT\""));
        assertTrue(unknown.contains("\"errorCode\":\"UNKNOWN_UDF_ARGUMENT\""));
        assertTrue(malformed.contains("\"jobId\":null"));
        assertEquals(0, handlers.sampleCount.get());
        assertEquals(0, handlers.trainCount.get());
        assertEquals(0, handlers.detectCount.get());
    }

    @Test
    void shouldConvertHandlerExceptionToFailedResult() {
        RahaDetectUdfHandler handler = request -> {
            throw new IllegalStateException("模拟检测服务失败");
        };

        String result = new F_DW_RAHADETECT(handler)
                .call(detectRequest("failure-job"));

        assertTrue(result.contains("\"status\":\"FAILED\""));
        assertTrue(result.contains("\"jobId\":\"failure-job\""));
        assertTrue(result.contains("\"errorCode\":\"UDF_EXECUTION_FAILED\""));
        assertTrue(result.contains("\"errorMessage\":\"IllegalStateException\""));
    }

    @Test
    void shouldRegisterAndExecuteThreeDirectHandlersAsSparkSqlUdf() {
        SparkSession spark = SparkTestSession.get();
        StaticHandlers handlers = new StaticHandlers();
        new RahaUdfRegistrar().register(spark, handlers, handlers, handlers);
        Dataset<Row> requests = spark.createDataset(Collections.singletonList(
                detectRequest("spark-sql-job")), Encoders.STRING()).toDF("request");

        String result = requests.selectExpr(
                RahaUdfRegistrar.DETECT_FUNCTION + "(request) AS result")
                .first().getString(0);

        assertTrue(spark.catalog().functionExists(RahaUdfRegistrar.TRAIN_FUNCTION));
        assertTrue(spark.catalog().functionExists(RahaUdfRegistrar.DETECT_FUNCTION));
        assertTrue(spark.catalog().functionExists(RahaUdfRegistrar.SAMPLE_FUNCTION));
        assertTrue(result.contains("\"status\":\"SUCCEEDED\""));
        assertTrue(result.contains("\"jobId\":\"spark-sql-job\""));
    }

    @Test
    void shouldNotExposeNoArgEntryConstructors() {
        assertThrows(NoSuchMethodException.class,
                () -> F_DW_RAHASAMPLE.class.getConstructor());
        assertThrows(NoSuchMethodException.class,
                () -> F_DW_RAHATRAIN.class.getConstructor());
        assertThrows(NoSuchMethodException.class,
                () -> F_DW_RAHADETECT.class.getConstructor());
    }

    private static String sampleRequest(String jobId) {
        Map<String, String> values = commonValues(jobId);
        values.put("labelingBudget", "10");
        return FormDataCodec.encode(values);
    }

    private static String trainRequest(String jobId) {
        return FormDataCodec.encode(trainValues(jobId));
    }

    private static Map<String, String> trainValues(String jobId) {
        Map<String, String> values = commonValues(jobId);
        values.put("annotationReference", "raha_labels");
        return values;
    }

    private static String detectRequest(String jobId) {
        Map<String, String> values = commonValues(jobId);
        values.put("modelVersion", "model-v1");
        return FormDataCodec.encode(values);
    }

    private static Map<String, String> commonValues(String jobId) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("datasetId", "dataset");
        values.put("inputReference", "source_table");
        values.put("sourceType", "TABLE");
        values.put("rowIdColumn", "id");
        values.put("snapshotId", "snapshot-v1");
        values.put("idempotencyKey", jobId);
        values.put("caller", "tester");
        values.put("resultTable", "raha_result_table");
        return values;
    }

    private static RahaUdfExecutionResult completed(String jobId,
                                                     RahaTaskType taskType,
                                                     String canonicalConfiguration) {
        Map<String, String> details = new LinkedHashMap<String, String>();
        details.put("processedCount", "1");
        RahaTaskSummary summary = new RahaTaskSummary(
                1000L, 1001L, 1L, 1L, 0L, 0L, details);
        return RahaUdfExecutionResult.completed(jobId, taskType,
                "repository://result/" + jobId,
                HashUtils.sha256Hex(canonicalConfiguration), summary);
    }

    /** 验证三个入口只调用各自强类型 handler。 */
    private static class TrackingHandlers implements RahaSampleUdfHandler,
            RahaTrainUdfHandler, RahaDetectUdfHandler {
        /** 采样执行次数。 */
        private final AtomicInteger sampleCount = new AtomicInteger();
        /** 训练执行次数。 */
        private final AtomicInteger trainCount = new AtomicInteger();
        /** 检测执行次数。 */
        private final AtomicInteger detectCount = new AtomicInteger();

        @Override
        public RahaUdfExecutionResult handle(RahaSampleUdfRequest request) {
            sampleCount.incrementAndGet();
            return completed(request.getIdempotencyKey(), RahaTaskType.SAMPLE,
                    request.toCanonicalConfiguration());
        }

        @Override
        public RahaUdfExecutionResult handle(RahaTrainUdfRequest request) {
            trainCount.incrementAndGet();
            return completed(request.getIdempotencyKey(), RahaTaskType.TRAIN,
                    request.toCanonicalConfiguration());
        }

        @Override
        public RahaUdfExecutionResult handle(RahaDetectUdfRequest request) {
            detectCount.incrementAndGet();
            return completed(request.getIdempotencyKey(), RahaTaskType.DETECT,
                    request.toCanonicalConfiguration());
        }
    }

    /** Spark 序列化测试使用的无外部状态 handler。 */
    private static final class StaticHandlers extends TrackingHandlers {
        /** Java 序列化版本。 */
        private static final long serialVersionUID = 1L;
    }
}
