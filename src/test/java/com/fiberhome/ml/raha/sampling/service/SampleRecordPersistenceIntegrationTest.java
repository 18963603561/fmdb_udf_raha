package com.fiberhome.ml.raha.sampling.service;

import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.RowIdentityResult;
import com.fiberhome.ml.raha.data.loader.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.fmdb.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.fmdb.FmdbPhysicalTable;
import com.fiberhome.ml.raha.fmdb.FmdbSampleRecordRepository;
import com.fiberhome.ml.raha.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证一千行 c1 的原值、模式、幂等追加、开关和跨实例读取。
 */
class SampleRecordPersistenceIntegrationTest {

    /** 每个测试隔离的内存 FMDB 网关。 */
    private InMemoryFmdbTableGateway gateway;

    @BeforeEach
    void setUp() {
        gateway = new InMemoryFmdbTableGateway(SparkTestSession.get());
    }

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldPersistOneThousandRowsIdempotentlyAndReloadAfterRestart() {
        Fixture fixture = fixture(1000);
        FmdbPersistenceConfig config = FmdbPersistenceConfig.fromDefaults();
        FmdbSampleRecordRepository repository = new FmdbSampleRecordRepository(
                SparkTestSession.get(), gateway, config);
        SampleRecordService service = new SampleRecordService(repository,
                fixedClock(2000L));

        SampleMaterializationResult first = service.materializeAndPersist(
                fixture.dataset, fixture.snapshot, fixture.identityConfig,
                fixture.sampling);
        SampleMaterializationResult repeated = service.materializeAndPersist(
                fixture.dataset, fixture.snapshot, fixture.identityConfig,
                fixture.sampling);
        FmdbSampleRecordRepository restarted = new FmdbSampleRecordRepository(
                SparkTestSession.get(), gateway, config);
        SampleBatch loaded = restarted.find(fixture.dataset.getDatasetId(),
                        first.getBatch().getPartitionMonth(),
                        first.getBatch().getSampleBatchId()).get();
        List<com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow>
                annotationRows = restarted.findForAnnotation(
                fixture.dataset.getDatasetId(),
                first.getBatch().getPartitionMonth(),
                first.getBatch().getSampleBatchId());

        assertTrue(first.isPersisted());
        assertEquals(1000L, first.getWrittenCount());
        assertEquals(0L, repeated.getWrittenCount());
        assertEquals(1000, loaded.getRecords().size());
        assertEquals(1000, annotationRows.size());
        assertEquals(1000L, gateway.read(
                FmdbPhysicalTable.SAMPLE_RECORD.getTableName()).count());
        assertEquals("value-1", findValue(loaded, "1"));
        assertFalse(loaded.getRecords().get(0).getRowData().containsKey(
                RowIdentityColumns.ROW_ID));
        assertNotNull(loaded.getRecords().get(0).getColumnSchema().get("columns"));
        assertEquals(fixture.dataset.getSchemaHash(),
                loaded.getRecords().get(0).getSchemaHash());
        Map<String, String> expectedHashes = new LinkedHashMap<String, String>();
        for (Row row : fixture.dataset.getDataFrame().select(
                RowIdentityColumns.ROW_ID,
                RowIdentityColumns.ROW_CONTENT_HASH).collectAsList()) {
            expectedHashes.put(row.getString(0), row.getString(1));
        }
        Map<String, String> expectedTasks = new LinkedHashMap<String, String>();
        for (AnnotationTask task : fixture.sampling.getTasks()) {
            expectedTasks.put(task.getRowId(), task.getTaskId());
        }
        for (com.fiberhome.ml.raha.sampling.domain.SampleRecord record
                : loaded.getRecords()) {
            assertEquals(expectedHashes.get(record.getRowId()),
                    record.getRowContentHash());
            assertEquals(expectedTasks.get(record.getRowId()),
                    record.getSamplingContext().get("annotationTaskId"));
        }
    }

    @Test
    void shouldMaterializeWithoutWritingWhenSampleTableIsDisabled() {
        Fixture fixture = fixture(3);
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder()
                .table(FmdbPhysicalTable.ANNOTATION_RECORD, false)
                .table(FmdbPhysicalTable.SAMPLE_RECORD, false)
                .build();
        SampleRecordService service = new SampleRecordService(
                new FmdbSampleRecordRepository(SparkTestSession.get(), gateway, config),
                fixedClock(2000L));

        SampleMaterializationResult result = service.materializeAndPersist(
                fixture.dataset, fixture.snapshot, fixture.identityConfig,
                fixture.sampling);

        assertFalse(result.isPersisted());
        assertEquals(0L, result.getWrittenCount());
        assertEquals(3, result.getBatch().getRecords().size());
        assertFalse(gateway.tableExists(
                FmdbPhysicalTable.SAMPLE_RECORD.getTableName()));
    }

    @Test
    void shouldFailWhenSamplingTaskCannotFindTrustedRow() {
        Fixture fixture = fixture(3);
        List<AnnotationTask> tasks = new ArrayList<AnnotationTask>(
                fixture.sampling.getTasks());
        Map<String, String> clusters = new LinkedHashMap<String, String>();
        clusters.put("value", "cluster-missing");
        tasks.add(new AnnotationTask("task-missing", "sample-job",
                "missing-row-id", 1, 1.0d, clusters, "sampling-v1",
                1000L, 5000L));
        SamplingBatchResult invalid = new SamplingBatchResult(
                Collections.emptyList(), tasks, "sampling-v1",
                new SamplingMetrics(4L, 0L, 4L));
        SampleRecordService service = new SampleRecordService(
                new FmdbSampleRecordRepository(SparkTestSession.get(), gateway,
                        FmdbPersistenceConfig.fromDefaults()), fixedClock(2000L));

        assertThrows(IllegalStateException.class,
                () -> service.materializeAndPersist(fixture.dataset,
                        fixture.snapshot, fixture.identityConfig, invalid));
        assertFalse(gateway.tableExists(
                FmdbPhysicalTable.SAMPLE_RECORD.getTableName()));
    }

    private static String findValue(SampleBatch batch, String businessId) {
        for (com.fiberhome.ml.raha.sampling.domain.SampleRecord record
                : batch.getRecords()) {
            if (businessId.equals(record.getRowData().get("id"))) {
                return String.valueOf(record.getRowData().get("value"));
            }
        }
        return null;
    }

    private static Fixture fixture(int count) {
        SparkSession spark = SparkTestSession.get();
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true);
        List<Row> sourceRows = new ArrayList<Row>(count);
        for (int index = 1; index <= count; index++) {
            sourceRows.add(RowFactory.create(String.valueOf(index),
                    "value-" + index));
        }
        RowIdentityConfig identityConfig = RowIdentityConfig.sourceKey("id");
        RowIdentityResult identified = new RowIdentityService().identify(
                spark.createDataFrame(sourceRows, schema), identityConfig);
        String schemaHash = new SchemaHasher().hash(schema);
        RahaDataset dataset = new RahaDataset("sample-dataset", "snapshot-v1",
                "sample_table", RowIdentityColumns.ROW_ID,
                java.util.Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false, false, false),
                        new ColumnMetadata("value", 1, "string", true, true, false)),
                identified.getDataFrame(), schemaHash, Collections.emptyMap());
        DatasetSnapshot snapshot = new DatasetSnapshot("sample-dataset",
                "snapshot-v1", "source_table", "sample_table",
                RowIdentityColumns.ROW_ID, schemaHash, count, 2,
                "source-v1", 1000L);
        List<AnnotationTask> tasks = new ArrayList<AnnotationTask>(count);
        Map<String, String> clusters = new LinkedHashMap<String, String>();
        clusters.put("value", "cluster-v1");
        int taskIndex = 0;
        for (Row row : identified.getDataFrame().select(
                RowIdentityColumns.ROW_ID).collectAsList()) {
            String rowId = row.getString(0);
            tasks.add(new AnnotationTask("task-" + (++taskIndex), "sample-job",
                    rowId, 1, 1.0d, clusters, "sampling-v1", 1000L, 5000L));
        }
        SamplingBatchResult sampling = new SamplingBatchResult(
                Collections.emptyList(), tasks, "sampling-v1",
                new SamplingMetrics(count, 0L, count));
        return new Fixture(dataset, snapshot, identityConfig, sampling);
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    /** 测试所需的可信数据集、快照、身份配置和采样结果。 */
    private static final class Fixture {

        /** 已生成稳定行身份的数据集。 */
        private final RahaDataset dataset;
        /** 数据集快照。 */
        private final DatasetSnapshot snapshot;
        /** 行身份配置。 */
        private final RowIdentityConfig identityConfig;
        /** 待物化采样结果。 */
        private final SamplingBatchResult sampling;

        private Fixture(RahaDataset dataset,
                        DatasetSnapshot snapshot,
                        RowIdentityConfig identityConfig,
                        SamplingBatchResult sampling) {
            this.dataset = dataset;
            this.snapshot = snapshot;
            this.identityConfig = identityConfig;
            this.sampling = sampling;
        }
    }
}
