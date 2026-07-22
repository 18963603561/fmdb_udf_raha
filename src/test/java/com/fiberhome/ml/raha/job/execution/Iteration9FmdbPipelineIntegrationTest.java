package com.fiberhome.ml.raha.job.execution;

import com.fiberhome.ml.raha.cluster.algorithm.HierarchicalColumnClusterer;
import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.config.dto.FailureToleranceConfig;
import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.metadata.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdValidator;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.metadata.SchemaHasher;
import com.fiberhome.ml.raha.data.loader.metadata.SnapshotMetadataFactory;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.FeatureDictionaryVersioner;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationConfig;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationService;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.model.release.ColumnModelCompatibilityValidator;
import com.fiberhome.ml.raha.model.release.ColumnModelMetadataFactory;
import com.fiberhome.ml.raha.model.release.ColumnModelVersioner;
import com.fiberhome.ml.raha.model.release.ModelReleaseManager;
import com.fiberhome.ml.raha.model.release.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.model.training.ColumnTrainingDataBuilder;
import com.fiberhome.ml.raha.model.training.LogisticRegressionTrainingConfig;
import com.fiberhome.ml.raha.model.training.WeightedRuleFallbackTrainer;
import com.fiberhome.ml.raha.repository.adapter.DefaultCellLabelRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultColumnProfileRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultStrategyRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbDetectionWriteContext;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.DefaultFmdbSchemaResolver;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbSchemaResolver;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbFeatureDictionaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbWriteMode;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.detect.RahaDetectRequest;
import com.fiberhome.ml.raha.service.detect.RahaDetectService;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import com.fiberhome.ml.raha.service.train.RahaTrainRequest;
import com.fiberhome.ml.raha.service.train.RahaTrainService;
import com.fiberhome.ml.raha.strategy.api.StrategyRegistry;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutor;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证训练和检测核心流程能够通过 FMDB 适配层完成数据及产物读写。
 */
class Iteration9FmdbPipelineIntegrationTest {

    /** FMDB 输入视图。 */
    private static final String INPUT_TABLE = "raha_iteration9_input";
    /** FMDB 模型表。 */
    private static final String MODEL_TABLE = "raha_iteration9_models";
    /** FMDB 字典表。 */
    private static final String DICTIONARY_TABLE = "raha_iteration9_dictionaries";
    /** FMDB 检测结果表。 */
    private static final String RESULT_TABLE = "raha_iteration9_results";

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldTrainReloadAndDetectThroughFmdbAdapters() {
        SparkSession spark = SparkTestSession.get();
        spark.catalog().dropTempView(INPUT_TABLE);
        inputFrame(spark).createOrReplaceTempView(INPUT_TABLE);
        Clock clock = fixedClock();
        InMemoryFmdbTableGateway fmdbGateway = new InMemoryFmdbTableGateway(spark);
        InMemoryRahaRepository coreStorage = new InMemoryRahaRepository();
        RahaDataset dataset = new ColumnProfileService(new ColumnProfiler(),
                new DefaultColumnProfileRepository(coreStorage), clock)
                .profileAndSave(loadDataset(spark, clock), version("profile-stage"));
        StrategyRepository strategyRepository = new DefaultStrategyRepository(coreStorage);
        ColumnClusteringService clusteringService = new ColumnClusteringService(
                new HierarchicalColumnClusterer(new ClusterVersioner(), clock),
                new DefaultClusterRepository(coreStorage), clock);
        ModelMetadataRepository metadataRepository =
                new DefaultModelMetadataRepository(coreStorage);
        FmdbModelStore modelStore = new FmdbModelStore(spark, fmdbGateway,
                MODEL_TABLE, DICTIONARY_TABLE, clock,
                FmdbPersistenceConfig.fromDefaults());
        ModelReleaseManager releaseManager = new ModelReleaseManager(
                metadataRepository, clock);
        RahaTrainService trainService = new RahaTrainService(
                new StrategyPlanService(new StrategyPlanGenerator(),
                        strategyRepository, clock),
                new StrategyExecutionService(new StrategyExecutor(
                        StrategyRegistry.defaults(), clock), strategyRepository, clock),
                new FeatureService(new FeatureAssembler(
                        new FeatureDictionaryVersioner(), clock),
                        new DefaultFeatureRepository(coreStorage), clock),
                clusteringService, new LabelPropagationService(
                        new DefaultCellLabelRepository(coreStorage), clock),
                new ColumnTrainingDataBuilder(),
                new WeightedRuleFallbackTrainer(new ColumnModelVersioner()),
                modelStore, new ColumnModelMetadataFactory(clock),
                releaseManager, clock);

        RahaServiceResult<RahaTrainOutput> trained = trainService.train(
                new RahaTrainRequest("fmdb-train", "train-stage", dataset,
                        trainingConfig(), directLabels(dataset),
                        LabelPropagationMethod.HOMOGENEITY,
                        LabelPropagationConfig.defaults(),
                        LogisticRegressionTrainingConfig.defaults(), "raha",
                        version("train-stage")));

        assertEquals(JobStatus.SUCCEEDED, trained.getStatus());
        assertEquals(1, trained.getPayload().getCandidateModels().size());
        for (FeatureDictionary dictionary
                : trained.getPayload().getFeatures().getDictionaries().values()) {
            appendDictionary(spark, fmdbGateway, dictionary);
        }
        RahaColumnModel candidate = trained.getPayload()
                .getCandidateModels().get("code");
        assertEquals(com.fiberhome.ml.raha.data.type.ModelStatus.PUBLISHED,
                candidate.getStatus());

        // 使用新存储实例模拟服务重启，确保检测不依赖训练进程内缓存。
        FmdbModelStore restartedStore = new FmdbModelStore(spark, fmdbGateway,
                MODEL_TABLE, DICTIONARY_TABLE, clock,
                FmdbPersistenceConfig.fromDefaults());
        assertThrows(UnsupportedOperationException.class,
                () -> restartedStore.loadDictionary(trained.getPayload()
                        .getFeatures().getDictionaries().get("code").getVersion()));
        DetectionResultRepository detectionRepository =
                new DefaultDetectionResultRepository(coreStorage);
        RahaDetectService detectService = new RahaDetectService(
                new PublishedColumnModelLoader(metadataRepository, restartedStore,
                        new ColumnModelCompatibilityValidator()),
                new ColumnModelPredictor(), detectionRepository, clock);
        RahaServiceResult<RahaDetectOutput> detected = detectService.detect(
                new RahaDetectRequest("fmdb-detect", "detect-stage", "config-v1",
                        dataset, trained.getPayload().getFeatures(),
                        trained.getPayload().getStrategyPlanVersion(),
                        version("detect-stage"), ResourceConfig.defaults()));

        assertEquals(JobStatus.SUCCEEDED, detected.getStatus());
        assertEquals(8, detected.getPayload().getResults().size());
        FmdbPersistenceConfig idempotentConfig = FmdbPersistenceConfig.builder()
                .writeMode(FmdbWriteMode.IDEMPOTENT_BY_KEY)
                .build();
        SparkSqlFmdbResultWriter resultWriter = new SparkSqlFmdbResultWriter(
                spark, fmdbGateway, clock, idempotentConfig);
        long errorCount = detected.getPayload().getResults().stream()
                .filter(com.fiberhome.ml.raha.data.domain.DetectionResult::isError)
                .count();
        FmdbDetectionWriteContext writeContext = new FmdbDetectionWriteContext(
                "detect-batch-v1", INPUT_TABLE, "model-set-v1", sourceRows(dataset));
        assertEquals(errorCount, resultWriter.writeDetectionResults(
                RESULT_TABLE, writeContext, detected.getPayload().getResults()));
        assertEquals(errorCount, resultWriter.writeDetectionResults(
                RESULT_TABLE, writeContext, detected.getPayload().getResults()));
        assertEquals(errorCount * 2L, fmdbGateway.read(RESULT_TABLE).count());
        assertTrue(fmdbGateway.read(RESULT_TABLE)
                .filter("model_version IS NOT NULL").count() > 0L);
    }

    private static RahaDataset loadDataset(SparkSession spark, Clock clock) {
        FmdbDatasetLoader loader = new FmdbDatasetLoader(spark,
                new RowIdentityService(), new RowIdValidator(), new SchemaHasher(),
                new DefaultFmdbSchemaResolver(new ColumnMetadataFactory()),
                new SnapshotMetadataFactory(), clock);
        DataLoadRequest request = new DataLoadRequest("fmdb-dataset", INPUT_TABLE,
                INPUT_TABLE, RowIdentityConfig.sourceKey("id"),
                DataFormat.FMDB_TABLE,
                Collections.<String, String>emptyMap(),
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), "snapshot-v1", "source-v1");
        return loader.load(request).getDataset();
    }

    private static RahaJobConfig trainingConfig() {
        Set<String> strategyTypes = Collections.singleton(StrategyTypes.PVD_CHARACTER_SET);
        StrategyConfig strategyConfig = new StrategyConfig(
                EnumSet.of(StrategyFamily.PVD), 10,
                Collections.<String>emptySet(), Collections.<String>emptySet(),
                1, 60000L, false, strategyTypes,
                Collections.<String>emptySet(), Collections.<String, Integer>emptyMap());
        return new RahaJobConfig(JobType.TRAINING, "fmdb-dataset", "snapshot-v1",
                INPUT_TABLE, RowIdentityConfig.sourceKey("id"), 20260715L,
                strategyConfig,
                FeatureConfig.defaults(),
                new ModelConfig(ClassifierType.WEIGHTED_RULE, 0.5d, true),
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 2, 100),
                new SamplingConfig(2, true, false, 60000L),
                ResourceConfig.defaults(), FailureToleranceConfig.defaults());
    }

    private static org.apache.spark.sql.Dataset<Row> inputFrame(SparkSession spark) {
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("code", DataTypes.StringType, true);
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create("1", "ABC"), RowFactory.create("2", "ABC"),
                RowFactory.create("3", "ABC"), RowFactory.create("4", "ABC"),
                RowFactory.create("5", "ABC"), RowFactory.create("6", "ABC"),
                RowFactory.create("7", "BAD#"), RowFactory.create("8", "BAD#")), schema);
    }

    private static List<CellLabel> directLabels(RahaDataset dataset) {
        List<CellLabel> labels = new ArrayList<CellLabel>();
        labels.add(label(rowId(dataset, "1"), 0));
        labels.add(label(rowId(dataset, "7"), 1));
        return labels;
    }

    private static String rowId(RahaDataset dataset, String businessId) {
        return dataset.getDataFrame().filter("id = '" + businessId + "'")
                .select(dataset.getRowIdColumn()).first().getString(0);
    }

    private static CellLabel label(String rowId, int value) {
        String cellId = new CellCoordinate("fmdb-dataset", "snapshot-v1",
                rowId, "code").toCellId();
        return new CellLabel(cellId, value, LabelSource.GROUND_TRUTH,
                1.0d, null, null, "iteration-9-test", 1000L);
    }

    private static ArtifactVersion version(String stageId) {
        return new ArtifactVersion("config-v1", "snapshot-v1", stageId, 1);
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
    }

    private static Map<String, Map<String, Object>> sourceRows(RahaDataset dataset) {
        Map<String, Map<String, Object>> rows =
                new LinkedHashMap<String, Map<String, Object>>();
        for (Row row : dataset.getDataFrame().collectAsList()) {
            Map<String, Object> values = new LinkedHashMap<String, Object>();
            values.put("id", row.getAs("id"));
            values.put("code", row.getAs("code"));
            String rowId = row.getAs(dataset.getRowIdColumn());
            rows.put(rowId, values);
        }
        return rows;
    }

    private static void appendDictionary(SparkSession spark,
                                         InMemoryFmdbTableGateway gateway,
                                         FeatureDictionary dictionary) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("training_batch_id", "fmdb-train");
        values.put("dataset_id", "fmdb-dataset");
        values.put("source_version", "source-v1");
        values.put("schema_hash", "schema-v1");
        values.put("merge_algorithm_version", "direct-input-v1");
        values.put("training_context_json", "{}");
        values.put("column_name", dictionary.getColumnName());
        values.put("profile_version", null);
        values.put("profile_json", null);
        values.put("strategy_plan_version", "strategy-v1");
        values.put("strategy_plan_json", "{}");
        values.put("feature_dictionary_version", dictionary.getVersion());
        values.put("feature_dictionary_json",
                FmdbFeatureDictionaryCodec.write(dictionary));
        values.put("cluster_version", null);
        values.put("cluster_summary_json", null);
        values.put("propagation_summary_json", null);
        values.put("created_at", 1000L);
        gateway.append(DICTIONARY_TABLE,
                spark.createDataFrame(Collections.singletonList(
                                FmdbTableRecord.of(
                                        FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT,
                                        values).toRow()),
                        FmdbTableSchemas.schema(
                                FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT)),
                Arrays.asList("training_batch_id", "column_name"), 1L);
    }
}
