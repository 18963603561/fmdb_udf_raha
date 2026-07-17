package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.data.FeatureType;
import com.fiberhome.ml.raha.data.JobType;
import com.fiberhome.ml.raha.data.loader.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.DataValidationException;
import com.fiberhome.ml.raha.data.loader.LoadedDataset;
import com.fiberhome.ml.raha.data.loader.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.feature.FeatureDefinition;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.job.RahaJob;
import com.fiberhome.ml.raha.model.ColumnModelArtifact;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        assertEquals("id", table.getDataset().getRowIdColumn());
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
                spark, gateway, fixedClock(2000L));
        RahaJob job = new RahaJob("job-1", "request-1", JobType.DETECTION,
                "dataset", "snapshot-v1", "config-v1", 1000L);

        assertEquals(1L, writer.writeJob(JOB_TABLE, job));
        assertEquals(0L, writer.writeJob(JOB_TABLE, job));
        assertEquals(1L, gateway.read(JOB_TABLE).count());

        List<DetectionResult> results = Arrays.asList(
                detection("1", true, 0.9d), detection("2", false, 0.1d));
        assertEquals(2L, writer.writeDetectionResults(RESULT_TABLE, "job-1", results));
        assertEquals(0L, writer.writeDetectionResults(RESULT_TABLE, "job-1", results));
        assertEquals(2L, gateway.read(RESULT_TABLE).count());
        assertFalse(Arrays.asList(gateway.read(RESULT_TABLE).columns())
                .contains("correct_value"));
        assertEquals(1L, gateway.read(RESULT_TABLE)
                .filter("is_error = true").count());
    }

    @Test
    void shouldReloadImmutableModelAndFeatureDictionaryFromFmdb() {
        SparkSession spark = SparkTestSession.get();
        FmdbModelStore first = new FmdbModelStore(spark, gateway,
                MODEL_TABLE, DICTIONARY_TABLE, fixedClock(2000L));
        ColumnModelArtifact artifact = artifact(0.25d);
        FeatureDictionary dictionary = dictionary();

        String modelPath = first.save(artifact);
        String dictionaryPath = first.saveDictionary(dictionary);
        FmdbModelStore restarted = new FmdbModelStore(spark, gateway,
                MODEL_TABLE, DICTIONARY_TABLE, fixedClock(3000L));

        ColumnModelArtifact loadedModel = restarted.load(modelPath);
        FeatureDictionary loadedDictionary = restarted.loadDictionary(dictionary.getVersion());
        assertEquals(artifact.getModelVersion(), loadedModel.getModelVersion());
        assertEquals(artifact.getCoefficients(), loadedModel.getCoefficients());
        assertEquals(dictionary.getVersion(), loadedDictionary.getVersion());
        assertEquals(2, loadedDictionary.getDefinitions().size());
        assertTrue(dictionaryPath.startsWith("fmdb://" + DICTIONARY_TABLE + "/"));

        assertEquals(modelPath, restarted.save(artifact));
        assertEquals(dictionaryPath, restarted.saveDictionary(dictionary));
        assertEquals(1L, gateway.read(MODEL_TABLE).count());
        assertEquals(2L, gateway.read(DICTIONARY_TABLE).count());
        assertThrows(IllegalStateException.class,
                () -> restarted.save(artifact(0.75d)));
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
        return new FmdbDatasetLoader(SparkTestSession.get(), new RowIdValidator(),
                new SchemaHasher(), new DefaultFmdbSchemaResolver(
                new ColumnMetadataFactory()), new SnapshotMetadataFactory(),
                fixedClock(1000L));
    }

    private static DataLoadRequest request(DataFormat format, String inputReference) {
        return new DataLoadRequest("dataset", inputReference, INPUT_VIEW, "id", format,
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

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
