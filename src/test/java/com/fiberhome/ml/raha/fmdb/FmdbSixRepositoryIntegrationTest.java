package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.domain.RahaStage;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.ModelPersistenceContext;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbJobRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbStageRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingArtifactRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingCellRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbTrainingColumnArtifactRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbClusterSummaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbColumnArtifact;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbColumnProfileCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbFeatureDictionaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbStrategyArtifactCodec;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.repository.port.DetectionResultSaveContext;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证六类 FMDB 仓储能够使用最终物理表跨实例恢复。 */
class FmdbSixRepositoryIntegrationTest {

    /** 测试统一使用的业务版本。 */
    private static final ArtifactVersion VERSION = new ArtifactVersion(
            "config-v1", "snapshot-v1", "stage-v1", 1);

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldAppendAndRestoreLatestStageState() {
        InMemoryFmdbTableGateway gateway = gateway();
        FmdbPersistenceConfig config = config();
        SparkSqlFmdbResultWriter writer = writer(gateway, config, 5000L);
        FmdbJobRepository jobs = new FmdbJobRepository(writer, gateway, config);
        RahaJob job = new RahaJob("job-stage", "idem-stage", JobType.TRAINING,
                "dataset-1", "snapshot-v1", "config-v1", 1000L);
        jobs.save(job, 1000L);
        FmdbStageRepository repository = new FmdbStageRepository(
                SparkTestSession.get(), gateway, config);
        RahaStage stage = new RahaStage("stage-profile", "job-stage",
                StageType.PROFILE, 1);
        stage.start(2000L);

        assertEquals(SaveOutcome.CREATED, repository.save(stage, VERSION, 2000L));
        assertEquals(SaveOutcome.UNCHANGED, repository.save(stage, VERSION, 2100L));
        stage.succeed(3000L);
        assertEquals(SaveOutcome.UPDATED, repository.save(stage, VERSION, 3000L));

        FmdbStageRepository restarted = new FmdbStageRepository(
                SparkTestSession.get(), gateway, config);
        assertEquals(1, restarted.findByJobId("job-stage").size());
        assertEquals("SUCCEEDED", restarted.findByJobId("job-stage")
                .get(0).getStatus().name());
        assertEquals(2L, gateway.read(FmdbPhysicalTable.JOB_STAGE_ATTEMPT
                .getTableName()).count());
    }

    @Test
    void shouldRestoreProfileStrategyFeatureAndClusterFromTrainingTables() {
        InMemoryFmdbTableGateway gateway = gateway();
        FmdbPersistenceConfig config = config();
        CellCoordinate coordinate = new CellCoordinate("dataset-1", "snapshot-v1",
                "row-1", "value");
        ColumnProfile profile = profile();
        StrategyPlan plan = plan();
        StrategyRunSummary summary = summary();
        FeatureDictionary dictionary = dictionary();
        ClusterAssignment assignment = new ClusterAssignment(coordinate.toCellId(),
                "value", coordinate, "cluster-1", "hierarchical", "cluster-v1", 0.2d);
        ColumnClusteringResult clustering = new ColumnClusteringResult("value",
                "hierarchical", ClusteringDistanceMetric.COSINE, 2, 1, 7L,
                "cluster-v1", ColumnClusteringStatus.SUCCEEDED, "完成",
                Collections.singletonList(assignment), 2000L);
        Map<String, Object> context = new LinkedHashMap<String, Object>();
        context.put("trainingSnapshotId", "snapshot-v1");
        FmdbTrainingColumnArtifactRecord artifact =
                new FmdbTrainingColumnArtifactRecord("job-train", "dataset-1",
                        "source-v1", "schema-v1", "merge-v1",
                        FmdbJsonCodec.write(context), "value", "profile-v1",
                        FmdbColumnProfileCodec.write(profile), "strategy-v1",
                        FmdbStrategyArtifactCodec.write(
                                Collections.singletonList(plan),
                                Collections.singletonList(summary)),
                        dictionary.getVersion(),
                        FmdbFeatureDictionaryCodec.write(dictionary),
                        clustering.getClusterVersion(),
                        FmdbClusterSummaryCodec.write(clustering), null, 2000L);
        FmdbTrainingCellRecord cell = new FmdbTrainingCellRecord(
                "job-train", "dataset-1", "snapshot-v1", "row-1", "value",
                coordinate.toCellId(), "bad", dictionary.getVersion(),
                "{\"0\":1.0}", "{\"signal\":\"hit\"}", "cluster-1", 0.2d,
                null, null, null, null, null, 2000L);
        FmdbTrainingArtifactRepository writer = new FmdbTrainingArtifactRepository(
                SparkTestSession.get(), gateway, config);
        assertEquals(1L, writer.saveColumnArtifacts(
                Collections.singletonList(artifact)));
        assertEquals(1L, writer.saveTrainingCells(Collections.singletonList(cell)));

        FmdbColumnProfileRepository profiles =
                new FmdbColumnProfileRepository(gateway, config);
        FmdbStrategyRepository strategies = new FmdbStrategyRepository(gateway, config);
        FmdbFeatureRepository features = new FmdbFeatureRepository(gateway, config);
        FmdbClusterRepository clusters = new FmdbClusterRepository(gateway, config);

        assertEquals(profile.getTotalCount(), profiles.find(
                "dataset-1", "snapshot-v1", "value").get().getTotalCount());
        assertEquals(plan.getStrategyId(), strategies.findPlans(
                "dataset-1", "snapshot-v1").get(0).getStrategyId());
        assertEquals(summary.getStrategyId(), strategies.findSummaries(
                "job-train").get(0).getStrategyId());
        assertTrue(strategies.findHits("job-train").isEmpty());
        assertEquals(dictionary.getVersion(), features.findDictionary(
                "job-train", "value").get().getVersion());
        assertEquals(1.0d, features.findRows("job-train", "value")
                .get(0).getValues().get(0));
        assertEquals(1, clusters.findAssignments(
                "job-train", "value", "cluster-v1").size());
        assertEquals(7L, clusters.findResult("job-train", "value",
                "cluster-v1").get().getRandomSeed());
    }

    @Test
    void shouldWriteOnlyErrorsAndRestoreDetectionResult() {
        InMemoryFmdbTableGateway gateway = gateway();
        FmdbPersistenceConfig config = config();
        Clock clock = fixedClock(5000L);
        FmdbModelStore modelStore = new FmdbModelStore(SparkTestSession.get(), gateway,
                FmdbPhysicalTable.MODEL_ARTIFACT.getTableName(),
                FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT.getTableName(),
                clock, config);
        modelStore.save(new ColumnModelArtifact("model", "model-v1", "value",
                        ClassifierType.LOGISTIC_REGRESSION, "dict-v1", 1, 0.5d,
                        0.0d, Collections.singletonMap(0, 1.0d), "SPARK_MLLIB"),
                new ModelPersistenceContext("model-set-v1", "dataset-1", "schema-v1",
                        "job-train", ModelStatus.PUBLISHED, "strategy-v1", "merge-v1",
                        Collections.singletonMap("accuracy", 1.0d), 4000L, 4000L));
        RahaDataset dataset = detectionDataset();
        CellCoordinate coordinate = new CellCoordinate("dataset-1", "snapshot-v1",
                "row-1", "value");
        DetectionResult error = detection("job-detect", coordinate, true, 0.9d);
        DetectionResult normal = detection("job-detect", coordinate, false, 0.1d);
        FmdbDetectionResultRepository repository =
                new FmdbDetectionResultRepository(writer(gateway, config, 5000L),
                        gateway, config);

        repository.saveAll(new DetectionResultSaveContext("job-detect", dataset,
                        Collections.singletonList("model-v1")),
                Arrays.asList(error, normal), VERSION, 5000L);

        FmdbDetectionResultRepository restarted =
                new FmdbDetectionResultRepository(writer(gateway, config, 6000L),
                        gateway, config);
        assertEquals(1, restarted.findByJob("job-detect").size());
        DetectionResult restored = restarted.find("job-detect",
                coordinate.toCellId()).get();
        assertTrue(restored.isError());
        assertEquals("snapshot-v1", restored.getCoordinate().getSnapshotId());
        assertEquals("model-v1", restored.getModelVersion());
        assertFalse(restored.getReasons().isEmpty());
    }

    private static FmdbPersistenceConfig config() {
        return FmdbPersistenceConfig.builder()
                .table(FmdbPhysicalTable.TRAINING_CELL, true)
                .columnArtifact(FmdbColumnArtifact.CLUSTER_SUMMARY, true)
                .build();
    }

    private static InMemoryFmdbTableGateway gateway() {
        return new InMemoryFmdbTableGateway(SparkTestSession.get());
    }

    private static SparkSqlFmdbResultWriter writer(
            InMemoryFmdbTableGateway gateway,
            FmdbPersistenceConfig config,
            long millis) {
        return new SparkSqlFmdbResultWriter(SparkTestSession.get(), gateway,
                fixedClock(millis), config);
    }

    private static ColumnProfile profile() {
        return new ColumnProfile("value", 1L, 0L, 0L, 1L,
                3, 3, 3.0d, 0L, 0.0d, null, null, null, null,
                null, null, Collections.singletonMap("TEXT", 1L),
                Collections.singletonMap(HashUtils.md5Hex("bad"), 1L));
    }

    private static StrategyPlan plan() {
        return new StrategyPlan("strategy-1", StrategyFamily.OD,
                Collections.singletonList("value"),
                Collections.singletonMap("type", "low-frequency"), 10,
                StrategyStatus.PLANNED);
    }

    private static StrategyRunSummary summary() {
        return new StrategyRunSummary("job-train", "stage-strategy", "snapshot-v1",
                "strategy-1", plan().getConfigurationHash(), StrategyFamily.OD,
                StrategyStatus.SUCCEEDED, 1L, 1L, 10L, null, null, 2000L);
    }

    private static FeatureDictionary dictionary() {
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(0, "strategy.hit",
                FeatureType.BINARY, "strategy-1", 0.0d));
        return new FeatureDictionary("dict-v1", "value", definitions, 2000L);
    }

    private static RahaDataset detectionDataset() {
        StructType schema = new StructType()
                .add("row_id", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true);
        Row row = RowFactory.create("row-1", "bad");
        return new RahaDataset("dataset-1", "snapshot-v1", "orders", "row_id",
                Arrays.asList(new ColumnMetadata("row_id", 0, "string", false,
                                false, false),
                        new ColumnMetadata("value", 1, "string", true, true, false)),
                SparkTestSession.get().createDataFrame(
                        Collections.singletonList(row), schema),
                "schema-v1", Collections.emptyMap());
    }

    private static DetectionResult detection(String jobId,
                                             CellCoordinate coordinate,
                                             boolean error,
                                             double score) {
        return new DetectionResult(jobId, "config-v1", "stage-detect", coordinate,
                HashUtils.md5Hex("bad"), "***", error, score, 0.5d,
                Collections.singletonList("strategy-1"),
                Collections.singletonMap("reason", "score"), "model", "model-v1",
                "dict-v1", 5000L);
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
