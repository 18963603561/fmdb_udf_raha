package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.loader.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.DataValidationException;
import com.fiberhome.ml.raha.data.loader.LoadedDataset;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.ModelPersistenceContext;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 使用本地 Spark Catalog 验证 FMDB 数据、结果、模型和字典适配闭环。
 */
class FmdbAdapterIntegrationTest {

    /** 测试使用的 FMDB 输入视图。 */
    private static final String INPUT_VIEW = "raha_fmdb_input";
    /** 测试任务表。 */
    private static final String JOB_TABLE = "raha_fmdb_jobs";
    /** 测试检测结果表。 */
    private static final String RESULT_TABLE = "raha_fmdb_results";
    /** 测试模型表。 */
    private static final String MODEL_TABLE = "raha_fmdb_models";
    /** 测试字典表。 */
    private static final String DICTIONARY_TABLE = "raha_fmdb_dictionaries";
    /** 验证连续追加后血缘被截断的测试表。 */
    private static final String LINEAGE_TABLE = "raha_fmdb_lineage";
    /** 每个测试独立使用的内存 FMDB 表网关。 */
    private InMemoryFmdbTableGateway gateway;

    @BeforeEach
    void prepareCatalog() {
        SparkSession spark = SparkTestSession.get();
        spark.catalog().dropTempView(INPUT_VIEW);
        spark.catalog().dropTempView(JOB_TABLE);
        spark.catalog().dropTempView(RESULT_TABLE);
        spark.catalog().dropTempView(MODEL_TABLE);
        spark.catalog().dropTempView(DICTIONARY_TABLE);
        spark.catalog().dropTempView(LINEAGE_TABLE);
        gateway = new InMemoryFmdbTableGateway(spark);
        inputFrame(spark).createOrReplaceTempView(INPUT_VIEW);
    }

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldLoadFmdbTableAndReadonlySqlWithStableSnapshot() {
        FmdbDatasetLoader loader = loader();

        LoadedDataset table = loader.load(request(DataFormat.FMDB_TABLE, INPUT_VIEW));
        LoadedDataset sql = loader.load(request(DataFormat.FMDB_SQL,
                "SELECT id, code FROM " + INPUT_VIEW));

        assertEquals(3L, table.getSnapshot().getRowCount());
        assertEquals(2, table.getSnapshot().getColumnCount());
        assertEquals("snapshot-v1", table.getDataset().getSnapshotId());
        assertEquals(RowIdentityColumns.ROW_ID,
                table.getDataset().getRowIdColumn());
        assertFalse(table.getDataset().getColumns().get(0).isDetectable());
        assertTrue(table.getDataset().getColumns().get(1).isDetectable());
        assertEquals(table.getDataset().getSchemaHash(),
                sql.getDataset().getSchemaHash());
        assertEquals(3L, sql.getDataset().getDataFrame().count());

        assertThrows(DataValidationException.class,
                () -> loader.load(request(DataFormat.FMDB_SQL,
                        "DROP TABLE " + INPUT_VIEW)));
    }

    @Test
    void shouldWriteJobsAndDetectionResultsIdempotently() {
        SparkSession spark = SparkTestSession.get();
        SparkSqlFmdbResultWriter writer = new SparkSqlFmdbResultWriter(
                spark, gateway, fixedClock(2000L),
                FmdbPersistenceConfig.fromDefaults());
        RahaJob job = new RahaJob("job-1", "request-1", JobType.DETECTION,
                "dataset", "snapshot-v1", "config-v1", 1000L);

        assertEquals(1L, writer.writeJob(JOB_TABLE, job,
                Collections.<String, Object>singletonMap("errorCount", 1L)));
        assertEquals(0L, writer.writeJob(JOB_TABLE, job,
                Collections.<String, Object>singletonMap("errorCount", 1L)));
        assertEquals(1L, gateway.read(JOB_TABLE).count());

        List<DetectionResult> results = Arrays.asList(
                detection("1", true, 0.9d), detection("2", false, 0.1d));
        FmdbDetectionWriteContext context = detectionContext("1", "2");
        assertEquals(1L, writer.writeDetectionResults(RESULT_TABLE, context, results));
        assertEquals(0L, writer.writeDetectionResults(RESULT_TABLE, context, results));
        assertEquals(1L, gateway.read(RESULT_TABLE).count());
        assertEquals("VALUE-1", gateway.read(RESULT_TABLE).first()
                .getAs("original_value"));
    }

    @Test
    void shouldSkipResultWritesWhenPersistenceIsDisabled() {
        SparkSession spark = SparkTestSession.get();
        SparkSqlFmdbResultWriter writer = new SparkSqlFmdbResultWriter(
                spark, gateway, fixedClock(2000L),
                FmdbPersistenceConfig.builder().enabled(false).build());
        RahaJob job = new RahaJob("job-disabled", "request-disabled", JobType.DETECTION,
                "dataset", "snapshot-v1", "config-v1", 1000L);

        assertEquals(0L, writer.writeJob(JOB_TABLE, job,
                Collections.<String, Object>emptyMap()));
        assertEquals(0L, writer.writeDetectionResults(RESULT_TABLE,
                detectionContext(),
                Collections.<DetectionResult>emptyList()));
        assertFalse(gateway.tableExists(JOB_TABLE));
        assertFalse(gateway.tableExists(RESULT_TABLE));
    }

    @Test
    void shouldDisableDetectionTableWithoutDisablingJobTable() {
        SparkSession spark = SparkTestSession.get();
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder()
                .table(FmdbPhysicalTable.DETECTION_RESULT, false)
                .build();
        SparkSqlFmdbResultWriter writer = new SparkSqlFmdbResultWriter(
                spark, gateway, fixedClock(2000L), config);
        RahaJob job = new RahaJob("job-1", "request-table-switch",
                JobType.DETECTION, "dataset", "snapshot-v1", "config-v1", 1000L);

        assertEquals(1L, writer.writeJob(JOB_TABLE, job,
                Collections.<String, Object>emptyMap()));
        assertEquals(0L, writer.writeDetectionResults(RESULT_TABLE,
                detectionContext("1"),
                Collections.singletonList(detection("1", true, 0.9d))));
        assertTrue(gateway.tableExists(JOB_TABLE));
        assertFalse(gateway.tableExists(RESULT_TABLE));
    }

    @Test
    void shouldReloadImmutableModelAndFeatureDictionaryFromFmdb() {
        SparkSession spark = SparkTestSession.get();
        FmdbModelStore first = new FmdbModelStore(spark, gateway,
                MODEL_TABLE, DICTIONARY_TABLE, fixedClock(2000L),
                FmdbPersistenceConfig.fromDefaults());
        ColumnModelArtifact artifact = artifact(0.25d);
        FeatureDictionary dictionary = dictionary();

        appendDictionary(spark, dictionary);
        String modelPath = first.save(artifact, modelContext());
        FmdbModelStore restarted = new FmdbModelStore(spark, gateway,
                MODEL_TABLE, DICTIONARY_TABLE, fixedClock(3000L),
                FmdbPersistenceConfig.fromDefaults());

        ColumnModelArtifact loadedModel = restarted.load(modelPath);
        FeatureDictionary loadedDictionary = restarted.loadDictionary(dictionary.getVersion());
        assertEquals(artifact.getModelVersion(), loadedModel.getModelVersion());
        assertEquals(artifact.getCoefficients(), loadedModel.getCoefficients());
        assertEquals(dictionary.getVersion(), loadedDictionary.getVersion());
        assertEquals(2, loadedDictionary.getDefinitions().size());

        assertEquals(modelPath, restarted.save(artifact, modelContext()));
        assertEquals(1L, gateway.read(MODEL_TABLE).count());
        assertEquals(1L, gateway.read(DICTIONARY_TABLE).count());
        assertThrows(IllegalStateException.class,
                () -> restarted.save(artifact(0.75d), modelContext()));
    }

    @Test
    void shouldKeepModelInCurrentCacheWhenModelTableIsDisabled() {
        SparkSession spark = SparkTestSession.get();
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder()
                .table(FmdbPhysicalTable.MODEL_ARTIFACT, false)
                .build();
        FmdbModelStore current = new FmdbModelStore(spark, gateway,
                MODEL_TABLE, DICTIONARY_TABLE, fixedClock(2000L), config);
        ColumnModelArtifact artifact = artifact(0.25d);

        String modelPath = current.save(artifact, modelContext());

        assertEquals(artifact.getModelVersion(),
                current.load(modelPath).getModelVersion());
        assertFalse(gateway.tableExists(MODEL_TABLE));

        FmdbModelStore restarted = new FmdbModelStore(spark, gateway,
                MODEL_TABLE, DICTIONARY_TABLE, fixedClock(3000L), config);
        assertThrows(IllegalStateException.class,
                () -> restarted.load(modelPath));
    }

    @Test
    void shouldMaterializeAndTruncateLineageAfterEachAppend() {
        SparkSession spark = SparkTestSession.get();
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true);
        org.apache.spark.sql.Dataset<Row> first = spark.createDataFrame(
                Collections.singletonList(RowFactory.create("1", "A")), schema);
        org.apache.spark.sql.Dataset<Row> second = spark.createDataFrame(
                Collections.singletonList(RowFactory.create("2", "B")), schema);

        assertEquals(1L, gateway.appendIdempotent(
                LINEAGE_TABLE, first, Collections.singletonList("id")));
        assertEquals(1L, gateway.appendIdempotent(
                LINEAGE_TABLE, second, Collections.singletonList("id")));

        assertEquals(2L, gateway.read(LINEAGE_TABLE).count());
        assertFalse(gateway.read(LINEAGE_TABLE).queryExecution()
                .logical().toString().contains("Union"));
    }

    private static FmdbDatasetLoader loader() {
        return new FmdbDatasetLoader(SparkTestSession.get(),
                new RowIdentityService(), new RowIdValidator(),
                new SchemaHasher(), new DefaultFmdbSchemaResolver(
                new ColumnMetadataFactory()), new SnapshotMetadataFactory(),
                fixedClock(1000L));
    }

    private static DataLoadRequest request(DataFormat format, String inputReference) {
        return new DataLoadRequest("dataset", inputReference, INPUT_VIEW,
                RowIdentityConfig.sourceKey("id"), format,
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.singleton("code"), "snapshot-v1", "source-v1");
    }

    private static org.apache.spark.sql.Dataset<Row> inputFrame(SparkSession spark) {
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true);
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create("1", "A"),
                RowFactory.create("2", "B"),
                RowFactory.create("3", "BAD#")), schema);
    }

    private static DetectionResult detection(String rowId,
                                             boolean error,
                                             double score) {
        return new DetectionResult("job-1", "config-v1", "detect-stage",
                new CellCoordinate("dataset", "snapshot-v1", rowId, "code"),
                HashUtils.sha256Hex("value-" + rowId), "***", error, score, 0.5d,
                Collections.singletonList(HashUtils.sha256Hex("strategy")),
                Collections.singletonMap("reason", "test"), "raha-code",
                HashUtils.sha256Hex("model"), HashUtils.sha256Hex("dictionary"), 1000L);
    }

    private static ColumnModelArtifact artifact(double intercept) {
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        coefficients.put(0, 1.5d);
        coefficients.put(1, -0.5d);
        return new ColumnModelArtifact("raha-code", HashUtils.sha256Hex("model-v1"),
                "code", ClassifierType.LOGISTIC_REGRESSION,
                HashUtils.sha256Hex("dictionary-v1"), 2, 0.5d, intercept,
                coefficients, "fmdb-test");
    }

    private static FeatureDictionary dictionary() {
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(
                0, "strategy-hit", FeatureType.BINARY, "strategy", 0.0d));
        definitions.put(1, new FeatureDefinition(
                1, "value-frequency", FeatureType.NUMERIC, "context", 0.0d));
        return new FeatureDictionary(HashUtils.sha256Hex("dictionary-v1"),
                "code", definitions, 1000L);
    }

    private static ModelPersistenceContext modelContext() {
        return new ModelPersistenceContext(HashUtils.sha256Hex("model-set-v1"),
                "dataset", "training-batch-v1", ModelStatus.CANDIDATE,
                HashUtils.sha256Hex("strategy-plan-v1"), "merge-v1",
                Collections.singletonMap("f1", 1.0d), 2000L, null);
    }

    private static FmdbDetectionWriteContext detectionContext(String... rowIds) {
        Map<String, Map<String, Object>> rows =
                new LinkedHashMap<String, Map<String, Object>>();
        for (String rowId : rowIds) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("id", rowId);
            row.put("code", "VALUE-" + rowId);
            rows.put(rowId, row);
        }
        return new FmdbDetectionWriteContext("detect-batch-v1", INPUT_VIEW,
                HashUtils.sha256Hex("model-set-v1"), rows);
    }

    private void appendDictionary(SparkSession spark,
                                  FeatureDictionary dictionary) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("training_batch_id", "training-batch-v1");
        values.put("dataset_id", "dataset");
        values.put("source_version", "source-v1");
        values.put("schema_hash", HashUtils.sha256Hex("schema-v1"));
        values.put("merge_algorithm_version", "merge-v1");
        values.put("training_context_json", "{}");
        values.put("column_name", dictionary.getColumnName());
        values.put("profile_version", null);
        values.put("profile_json", null);
        values.put("strategy_plan_version", HashUtils.sha256Hex("strategy-plan-v1"));
        values.put("strategy_plan_json", "{}");
        values.put("feature_dictionary_version", dictionary.getVersion());
        values.put("feature_dictionary_json",
                FmdbFeatureDictionaryCodec.write(dictionary));
        values.put("cluster_version", null);
        values.put("cluster_summary_json", null);
        values.put("propagation_summary_json", null);
        values.put("created_at", 2000L);
        gateway.appendIdempotent(DICTIONARY_TABLE,
                spark.createDataFrame(Collections.singletonList(
                                FmdbTableRecord.of(
                                        FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT,
                                        values).toRow()),
                        FmdbTableSchemas.schema(
                                FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT)),
                Arrays.asList("training_batch_id", "column_name"));
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
