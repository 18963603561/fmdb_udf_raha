package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.fmdb.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.fmdb.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.job.RahaIdGenerator;
import com.fiberhome.ml.raha.repository.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.FormDataCodec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证三个 Raha 表级 UDF 的异步提交、幂等、参数和异常返回契约。
 */
class RahaTableUdfIntegrationTest {

    /** UDF 任务状态临时表。 */
    private static final String JOB_TABLE = "raha_udf_jobs";
    /** 当前测试 Spark 会话。 */
    private SparkSession spark;
    /** 当前测试真实仓储提交器。 */
    private RahaUdfJobSubmitter submitter;

    @BeforeEach
    void prepareSubmitter() {
        spark = SparkTestSession.get();
        spark.catalog().dropTempView(JOB_TABLE);
        InMemoryFmdbTableGateway gateway = new InMemoryFmdbTableGateway(spark);
        SparkSqlFmdbResultWriter writer = new SparkSqlFmdbResultWriter(
                spark, gateway, fixedClock());
        submitter = new RepositoryBackedRahaUdfJobSubmitter(
                new DefaultJobRepository(new InMemoryRahaRepository()), writer,
                JOB_TABLE, new SequentialIdGenerator(), fixedClock());
    }

    @AfterEach
    void clearRuntime() {
        RahaUdfRuntime.clear();
    }

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldSubmitTrainDetectAndSampleAsynchronouslyAndDeduplicate() {
        F_DW_RAHATRAIN train = new F_DW_RAHATRAIN(submitter);
        F_DW_RAHADETECT detect = new F_DW_RAHADETECT(submitter);
        F_DW_RAHASAMPLE sample = new F_DW_RAHASAMPLE(submitter);

        String acceptedTrain = train.call(request(RahaTaskType.TRAIN, "train-key"));
        String duplicateTrain = train.call(request(RahaTaskType.TRAIN, "train-key"));
        String acceptedDetect = detect.call(request(RahaTaskType.DETECT, "detect-key"));
        String acceptedSample = sample.call(request(RahaTaskType.SAMPLE, "sample-key"));

        assertTrue(acceptedTrain.contains("\"status\":\"ACCEPTED\""));
        assertTrue(acceptedTrain.contains("\"jobId\":\"job-1\""));
        assertTrue(duplicateTrain.contains("\"status\":\"DUPLICATE\""));
        assertTrue(duplicateTrain.contains("\"jobId\":\"job-1\""));
        assertTrue(acceptedDetect.contains("\"taskType\":\"DETECT\""));
        assertTrue(acceptedDetect.contains("\"jobId\":\"job-2\""));
        assertTrue(acceptedSample.contains("\"taskType\":\"SAMPLE\""));
        assertTrue(acceptedSample.contains("\"jobId\":\"job-3\""));
        assertEquals(3L, spark.table(JOB_TABLE).count());
        assertEquals(3L, spark.table(JOB_TABLE).filter("status = 'CREATED'").count());
    }

    @Test
    void shouldReturnTraceableRejectionForInvalidArgumentsAndConflict() {
        F_DW_RAHATRAIN train = new F_DW_RAHATRAIN(submitter);
        F_DW_RAHADETECT detect = new F_DW_RAHADETECT(submitter);
        F_DW_RAHASAMPLE sample = new F_DW_RAHASAMPLE(submitter);
        train.call(request(RahaTaskType.TRAIN, "conflict-key"));

        Map<String, String> conflict = requestValues(RahaTaskType.TRAIN, "conflict-key");
        conflict.put("inputReference", "other_table");
        String conflictResult = train.call(FormDataCodec.encode(conflict));
        String missingModel = detect.call(FormDataCodec.encode(
                commonValues("missing-model")));
        Map<String, String> invalidBudgetValues = commonValues("invalid-budget");
        invalidBudgetValues.put("labelingBudget", "0");
        String invalidBudget = sample.call(FormDataCodec.encode(invalidBudgetValues));
        String unknown = train.call(request(RahaTaskType.TRAIN, "unknown")
                + "&unexpected=value");
        String malformed = train.call("not-a-form-request");

        assertTrue(conflictResult.contains("\"errorCode\":\"IDEMPOTENCY_CONFLICT\""));
        assertTrue(missingModel.contains("\"errorCode\":\"INVALID_UDF_ARGUMENT\""));
        assertTrue(invalidBudget.contains("\"errorCode\":\"INVALID_UDF_ARGUMENT\""));
        assertTrue(unknown.contains("\"errorCode\":\"UNKNOWN_UDF_ARGUMENT\""));
        assertTrue(malformed.contains("\"status\":\"REJECTED\""));
        assertTrue(malformed.contains("\"jobId\":null"));
    }

    @Test
    void shouldConvertUnexpectedSubmitterFailureToStableResult() {
        RahaUdfJobSubmitter failedSubmitter = request -> {
            throw new IllegalStateException("模拟 FMDB 提交失败");
        };

        String result = new F_DW_RAHADETECT(failedSubmitter)
                .call(request(RahaTaskType.DETECT, "failure-key"));

        assertTrue(result.contains("\"status\":\"REJECTED\""));
        assertTrue(result.contains("\"errorCode\":\"UDF_SUBMISSION_FAILED\""));
        assertTrue(result.contains("\"errorMessage\":\"IllegalStateException\""));
    }

    @Test
    void shouldRegisterAndExecuteAsSparkSqlUdf() {
        new RahaUdfRegistrar().register(spark, new SerializableStaticSubmitter());
        // 清理驱动进程静态状态，验证执行器使用随 UDF 序列化的提交器。
        RahaUdfRuntime.clear();
        Dataset<Row> requests = spark.createDataset(Collections.singletonList(
                request(RahaTaskType.DETECT, "spark-sql-key")), Encoders.STRING())
                .toDF("request");

        String result = requests.selectExpr(
                RahaUdfRegistrar.DETECT_FUNCTION + "(request) AS result")
                .first().getString(0);

        assertTrue(spark.catalog().functionExists(RahaUdfRegistrar.TRAIN_FUNCTION));
        assertTrue(spark.catalog().functionExists(RahaUdfRegistrar.DETECT_FUNCTION));
        assertTrue(spark.catalog().functionExists(RahaUdfRegistrar.SAMPLE_FUNCTION));
        assertTrue(result.contains("\"status\":\"ACCEPTED\""));
        assertTrue(result.contains("\"jobId\":\"spark-sql-job\""));
        assertTrue(result.contains("\"taskType\":\"DETECT\""));
    }

    @Test
    void shouldReturnStableErrorWhenNoArgUdfRuntimeIsUnavailable() {
        RahaUdfRuntime.clear();

        String result = new F_DW_RAHASAMPLE()
                .call(request(RahaTaskType.SAMPLE, "runtime-missing"));

        assertTrue(result.contains("\"status\":\"REJECTED\""));
        assertTrue(result.contains("\"errorCode\":\"UDF_RUNTIME_UNAVAILABLE\""));
    }

    private static String request(RahaTaskType taskType, String idempotencyKey) {
        return FormDataCodec.encode(requestValues(taskType, idempotencyKey));
    }

    private static Map<String, String> requestValues(RahaTaskType taskType,
                                                      String idempotencyKey) {
        Map<String, String> values = commonValues(idempotencyKey);
        if (taskType == RahaTaskType.TRAIN) {
            values.put("annotationReference", "raha_labels");
        } else if (taskType == RahaTaskType.DETECT) {
            values.put("modelVersion", "model-v1");
        } else {
            values.put("labelingBudget", "10");
        }
        return values;
    }

    private static Map<String, String> commonValues(String idempotencyKey) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("datasetId", "dataset");
        values.put("inputReference", "source_table");
        values.put("sourceType", "TABLE");
        values.put("rowIdColumn", "id");
        values.put("snapshotId", "snapshot-v1");
        values.put("idempotencyKey", idempotencyKey);
        values.put("caller", "tester");
        values.put("resultTable", "raha_result_table");
        return values;
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
    }

    /**
     * 为测试生成可预测任务标识。
     */
    private static final class SequentialIdGenerator implements RahaIdGenerator {
        /** 当前任务序号。 */
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public String newJobId() {
            return "job-" + sequence.incrementAndGet();
        }

        @Override
        public String newStageId(String jobId, StageType stageType, int attemptId) {
            return "stage-" + attemptId;
        }
    }

    /**
     * 验证 Spark UDF 序列化的无外部状态提交器。
     */
    private static final class SerializableStaticSubmitter
            implements RahaUdfJobSubmitter, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public RahaUdfSubmissionResult submit(RahaUdfRequest request) {
            return RahaUdfSubmissionResult.accepted("spark-sql-job",
                    request.getTaskType(), "fmdb://result/spark-sql-job",
                    "config-v1", 1000L);
        }
    }
}
