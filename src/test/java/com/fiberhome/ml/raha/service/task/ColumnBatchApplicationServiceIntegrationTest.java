package com.fiberhome.ml.raha.service.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.validation.ConfigVersioner;
import com.fiberhome.ml.raha.config.validation.RahaConfigValidator;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.job.execution.RahaJobOrchestrator;
import com.fiberhome.ml.raha.job.execution.StageFailureDecider;
import com.fiberhome.ml.raha.job.id.DefaultRahaIdGenerator;
import com.fiberhome.ml.raha.job.id.IdempotencyKeyGenerator;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.repository.adapter.DefaultJobRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStageRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchExecutionCoordinator;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchOptions;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchPlanner;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchSchemaResolver;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * 验证列批父任务、子任务汇总和幂等复用闭环。
 */
class ColumnBatchApplicationServiceIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldExecuteThreeDetectionBatchesAndReuseParentSummary() {
        SparkSession spark = SparkTestSession.get();
        createWideTable(spark);
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1784730000000L),
                ZoneOffset.UTC);
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        StageRepository stageRepository = new DefaultStageRepository(storage);
        CapturingDetectionWorkflow workflow = new CapturingDetectionWorkflow();
        RahaJobOrchestrator orchestrator = new RahaJobOrchestrator(
                new RahaConfigValidator(), new ConfigVersioner(),
                new IdempotencyKeyGenerator(), new DefaultRahaIdGenerator(),
                new StageFailureDecider(), new DefaultJobRepository(storage),
                stageRepository, clock);
        ColumnBatchExecutionCoordinator coordinator =
                new ColumnBatchExecutionCoordinator(
                        new ColumnBatchSchemaResolver(spark),
                        new ColumnBatchPlanner(), clock);
        RahaTaskApplicationService service = new RahaTaskApplicationService(
                orchestrator, new RahaWorkflowRegistry(
                Collections.<RahaWorkflow>singletonList(workflow)),
                stageRepository, null, coordinator);
        RahaTaskExecutionRequest request = detectionRequest();

        RahaTaskExecutionResult first = service.execute(request);
        RahaDetectOutput output = first.getPayload(RahaDetectOutput.class);

        assertEquals(JobStatus.SUCCEEDED, first.getJob().getStatus());
        assertEquals(3, workflow.getBatchColumns().size());
        assertEquals(Arrays.asList("c1", "c2"),
                workflow.getBatchColumns().get(0));
        assertEquals(Collections.singletonList("c5"),
                workflow.getBatchColumns().get(2));
        assertEquals(5, output.getModelVersions().size());
        assertEquals(3, ((Number) first.getResultSummary().get(
                "columnBatchCount")).intValue());
        assertEquals(first.getJob().getJobId(),
                first.getResultSummary().get("detectionBatchId"));

        RahaTaskExecutionResult reused = service.execute(request);
        assertTrue(reused.isReused());
        assertEquals(first.getJob().getJobId(), reused.getJob().getJobId());
        assertEquals(3, workflow.getBatchColumns().size());
    }

    private static RahaTaskExecutionRequest detectionRequest() {
        RowIdentityConfig identity = RowIdentityConfig.contentHash();
        RahaJobConfig config = RahaJobConfig.defaults(JobType.DETECTION,
                "dataset", "wide_batch_table", identity)
                .withExecutionInputFingerprint("column-batch-parent");
        DataLoadRequest loadRequest = new DataLoadRequest("dataset",
                "wide_batch_table", "wide_batch_table", "wide_batch_table",
                identity, DataFormat.FMDB_TABLE,
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), null, "source-v1");
        return RahaTaskExecutionRequest.detection(config, loadRequest,
                "model-set-1", MissingModelPolicy.PARTIAL)
                .withColumnBatchOptions(new ColumnBatchOptions(
                        2, 1, false, false));
    }

    private static void createWideTable(SparkSession spark) {
        List<Row> rows = Collections.singletonList(
                RowFactory.create("1", "2", "3", "4", "5"));
        StructType schema = new StructType()
                .add("c1", DataTypes.StringType, true)
                .add("c2", DataTypes.StringType, true)
                .add("c3", DataTypes.StringType, true)
                .add("c4", DataTypes.StringType, true)
                .add("c5", DataTypes.StringType, true);
        spark.createDataFrame(rows, schema)
                .createOrReplaceTempView("wide_batch_table");
    }

    /** 记录子请求字段并返回轻量检测输出的测试工作流。 */
    private static final class CapturingDetectionWorkflow
            implements RahaWorkflow {

        /** 每次子任务实际接收的字段。 */
        private final List<List<String>> batchColumns =
                new ArrayList<List<String>>();

        @Override
        public JobType getJobType() {
            return JobType.DETECTION;
        }

        @Override
        public List<StageHandler> createStageHandlers(
                RahaTaskExecutionRequest request) {
            List<String> columns = new ArrayList<String>(
                    request.getDataLoadRequest().getIncludedColumns());
            batchColumns.add(Collections.unmodifiableList(columns));
            return Collections.<StageHandler>singletonList(
                    new DetectOutputStageHandler(columns));
        }

        List<List<String>> getBatchColumns() {
            return batchColumns;
        }
    }

    /** 为测试子任务生成字段模型版本映射。 */
    private static final class DetectOutputStageHandler
            implements StageHandler {

        /** 当前列批字段。 */
        private final Collection<String> columns;

        private DetectOutputStageHandler(Collection<String> columns) {
            this.columns = columns;
        }

        @Override
        public StageType getStageType() {
            return StageType.PREDICT;
        }

        @Override
        public StageResult execute(StageExecutionContext context) {
            Map<String, String> versions =
                    new LinkedHashMap<String, String>();
            for (String column : columns) {
                versions.put(column, "model-" + column);
            }
            context.getAttributes().put(StageAttributeKeys.DETECT_OUTPUT,
                    new RahaDetectOutput(
                            Collections.emptyList(), versions,
                            Collections.emptyMap()));
            return StageResult.success();
        }
    }
}
