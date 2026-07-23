package com.fiberhome.ml.raha.fmdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
import com.fiberhome.ml.raha.checkpoint.SnapshotCheckpointWriteSession;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ClusteringMetrics;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbSnapshotCheckpointRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.HashUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/** 验证快照检查点可以限量分批保存并按字段裁剪恢复。 */
class FmdbSnapshotCheckpointRepositoryIntegrationTest {

    /** 测试使用的固定创建时间。 */
    private static final long CREATED_AT = 1784730000000L;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldSaveOrcColumnBatchesAndRestoreOneColumn() throws Exception {
        SparkSession spark = SparkTestSession.get();
        RecordingGateway gateway = new RecordingGateway(spark);
        Path detailPath = Files.createTempDirectory("raha-checkpoint-");
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder()
                .snapshotCheckpointDetailBasePath(detailPath.toUri().toString())
                .snapshotCheckpointColumnBatchSize(1)
                .snapshotCheckpointOrcPartitionCount(1)
                .build();
        FmdbSnapshotCheckpointRepository repository =
                new FmdbSnapshotCheckpointRepository(spark, gateway, config);
        PreparedInput input = preparedInput();

        SnapshotCheckpointWriteSession session = repository.begin(
                "job-sample", input.dataset, input.snapshot, input.plans,
                "strategy-plan-v1", "config-v1",
                CREATED_AT);
        repository.saveColumnBatch(session, 1,
                Collections.singletonList("c01"),
                features(input, "c01"), clustering(input, "c01"));
        repository.saveColumnBatch(session, 2,
                Collections.singletonList("c02"),
                features(input, "c02"), clustering(input, "c02"));
        repository.complete(session, input.strategyBatch);

        assertEquals(Arrays.asList(5L, 3L, 2L),
                gateway.getAppendSizes());
        assertEquals(Arrays.asList("STRATEGY_PLAN", "MANIFEST"),
                gateway.getLastRecordTypes());
        assertEquals(10L, gateway.read(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT
                .getTableName()).count());

        SnapshotPreparedArtifacts full = repository.restore(
                "dataset-1", "snapshot-v1", "config-v1").get();
        assertEquals(3, full.getDataset().getColumns().size());
        assertEquals(2, full.getFeatures().getDictionaries().size());
        assertEquals(4, full.getFeatures().getRows().size());
        assertEquals(4L, full.getClustering().getMetrics().getAssignmentCount());

        SnapshotPreparedArtifacts partial = repository.restore(
                "dataset-1", "snapshot-v1", "config-v1",
                Collections.singleton("c01")).get();
        assertEquals(Arrays.asList("row_key", "c01"),
                columnNames(partial.getDataset().getColumns()));
        assertEquals(Collections.singleton("c01"),
                partial.getDataset().getProfiles().keySet());
        assertEquals(Collections.singleton("c01"),
                partial.getFeatures().getDictionaries().keySet());
        assertEquals(2, partial.getFeatures().getRows().size());
        assertEquals(1, partial.getClustering().getResults().size());
        assertEquals(2L,
                partial.getClustering().getMetrics().getAssignmentCount());
        assertEquals(1, partial.getStrategyPlans().size());
        assertEquals(Collections.singletonList("c01"),
                partial.getStrategyPlans().get(0).getTargetColumns());
        assertFalse(partial.getStrategyPlans().stream().anyMatch(
                plan -> plan.getStrategyFamily() == StrategyFamily.RVD));
    }

    private static FeatureAssemblyResult features(PreparedInput input,
                                                   String column) {
        Map<String, FeatureDictionary> dictionaries =
                Collections.singletonMap(column,
                        input.features.getDictionaries().get(column));
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        for (SparseFeatureRow row : input.features.getRows()) {
            if (column.equals(row.getColumnName())) {
                rows.add(row);
            }
        }
        return new FeatureAssemblyResult(dictionaries, rows,
                new FeatureAssemblyMetrics(rows.size(), 1L, 1L, 0L));
    }

    private static ClusteringBatchResult clustering(PreparedInput input,
                                                     String column) {
        ColumnClusteringResult result =
                input.clustering.getResults().get(column);
        return new ClusteringBatchResult(Collections.singletonMap(column,
                result), new ClusteringMetrics(1L, 1L,
                result.getAssignments().size(), 0L));
    }

    private static PreparedInput preparedInput() {
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("row_key", 0, "string", false,
                        false, false),
                new ColumnMetadata("c01", 1, "string", true, true, false),
                new ColumnMetadata("c02", 2, "string", true, true, false));
        Map<String, ColumnProfile> profiles =
                new LinkedHashMap<String, ColumnProfile>();
        profiles.put("c01", profile("c01"));
        profiles.put("c02", profile("c02"));
        RahaDataset dataset = new RahaDataset("dataset-1", "snapshot-v1",
                "dw.wide_table", "row_key", columns, null,
                "schema-v1", profiles);
        DatasetSnapshot snapshot = new DatasetSnapshot("dataset-1",
                "snapshot-v1", "dw.wide_table", "dw.wide_table", "row_key",
                "schema-v1", 2L, 3, "source-v1", CREATED_AT);

        List<StrategyPlan> plans = Arrays.asList(
                plan("strategy-c01", StrategyFamily.OD,
                        Collections.singletonList("c01")),
                plan("strategy-c02", StrategyFamily.OD,
                        Collections.singletonList("c02")),
                plan("strategy-rvd", StrategyFamily.RVD,
                        Arrays.asList("c01", "c02")));
        List<StrategyExecutionResult> executions =
                new ArrayList<StrategyExecutionResult>();
        for (StrategyPlan plan : plans) {
            executions.add(StrategyExecutionResult.summaryOnly(
                    summary(plan)));
        }

        Map<String, FeatureDictionary> dictionaries =
                new LinkedHashMap<String, FeatureDictionary>();
        List<SparseFeatureRow> featureRows = new ArrayList<SparseFeatureRow>();
        Map<String, ColumnClusteringResult> clusteringResults =
                new LinkedHashMap<String, ColumnClusteringResult>();
        for (String column : Arrays.asList("c01", "c02")) {
            FeatureDictionary dictionary = dictionary(column);
            dictionaries.put(column, dictionary);
            List<ClusterAssignment> assignments =
                    new ArrayList<ClusterAssignment>();
            for (String rowId : Arrays.asList("1", "2")) {
                CellCoordinate coordinate = new CellCoordinate("dataset-1",
                        "snapshot-v1", rowId, column);
                featureRows.add(new SparseFeatureRow(coordinate.toCellId(),
                        column, coordinate, HashUtils.md5Hex(rowId + column),
                        "***", dictionary.getVersion(),
                        Collections.singletonMap(0, 1.0d),
                        Collections.singletonMap("signal", "hit")));
                assignments.add(new ClusterAssignment(coordinate.toCellId(),
                        column, coordinate, "cluster-1", "hierarchical",
                        "cluster-" + column, 0.1d));
            }
            clusteringResults.put(column, new ColumnClusteringResult(column,
                    "hierarchical", ClusteringDistanceMetric.COSINE, 2, 1,
                    7L, "cluster-" + column,
                    ColumnClusteringStatus.SUCCEEDED, "完成", assignments,
                    CREATED_AT));
        }
        FeatureAssemblyResult features = new FeatureAssemblyResult(
                dictionaries, featureRows,
                new FeatureAssemblyMetrics(4L, 2L, 2L, 0L));
        ClusteringBatchResult clustering = new ClusteringBatchResult(
                clusteringResults, new ClusteringMetrics(2L, 2L, 4L, 0L));
        return new PreparedInput(dataset, snapshot, plans,
                new StrategyBatchResult(executions), features, clustering);
    }

    private static ColumnProfile profile(String column) {
        return new ColumnProfile(column, 2L, 0L, 0L, 2L,
                1, 1, 1.0d, 0L, 0.0d, null, null, null, null,
                null, null, Collections.singletonMap("TEXT", 2L),
                Collections.singletonMap(HashUtils.md5Hex(column), 2L));
    }

    private static StrategyPlan plan(String id,
                                     StrategyFamily family,
                                     List<String> columns) {
        return new StrategyPlan(id, family, columns,
                Collections.singletonMap("type", family.name()), 10,
                StrategyStatus.PLANNED);
    }

    private static StrategyRunSummary summary(StrategyPlan plan) {
        return new StrategyRunSummary("job-sample", "stage-strategy",
                "snapshot-v1", plan.getStrategyId(),
                plan.getConfigurationHash(), plan.getStrategyFamily(),
                StrategyStatus.SUCCEEDED, 0L, 2L, 10L,
                null, null, CREATED_AT);
    }

    private static FeatureDictionary dictionary(String column) {
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(0, "strategy.hit",
                FeatureType.BINARY, "strategy-" + column, 0.0d));
        return new FeatureDictionary("dictionary-" + column, column,
                definitions, CREATED_AT);
    }

    private static List<String> columnNames(List<ColumnMetadata> columns) {
        List<String> names = new ArrayList<String>();
        for (ColumnMetadata column : columns) {
            names.add(column.getName());
        }
        return names;
    }

    /** 保存测试所需的完整检查点输入。 */
    private static final class PreparedInput {

        /** 数据集元数据。 */
        private final RahaDataset dataset;
        /** 数据集快照。 */
        private final DatasetSnapshot snapshot;
        /** 策略计划。 */
        private final List<StrategyPlan> plans;
        /** 策略执行摘要。 */
        private final StrategyBatchResult strategyBatch;
        /** 特征产物。 */
        private final FeatureAssemblyResult features;
        /** 聚类产物。 */
        private final ClusteringBatchResult clustering;

        private PreparedInput(RahaDataset dataset,
                              DatasetSnapshot snapshot,
                              List<StrategyPlan> plans,
                              StrategyBatchResult strategyBatch,
                              FeatureAssemblyResult features,
                              ClusteringBatchResult clustering) {
            this.dataset = dataset;
            this.snapshot = snapshot;
            this.plans = plans;
            this.strategyBatch = strategyBatch;
            this.features = features;
            this.clustering = clustering;
        }
    }

    /** 记录每次检查点追加规模和记录类型的测试网关。 */
    private static final class RecordingGateway implements FmdbTableGateway {

        /** 实际保存数据的内存网关。 */
        private final InMemoryFmdbTableGateway delegate;
        /** 每次直接追加的预期记录数。 */
        private final List<Long> appendSizes = new ArrayList<Long>();
        /** 每次直接追加包含的记录类型。 */
        private final List<List<String>> appendRecordTypes =
                new ArrayList<List<String>>();

        private RecordingGateway(SparkSession sparkSession) {
            this.delegate = new InMemoryFmdbTableGateway(sparkSession);
        }

        @Override
        public boolean tableExists(String tableName) {
            return delegate.tableExists(tableName);
        }

        @Override
        public Dataset<Row> read(String tableName) {
            return delegate.read(tableName);
        }

        @Override
        public Dataset<Row> read(String tableName,
                                 List<String> columns,
                                 Column condition) {
            return delegate.read(tableName, columns, condition);
        }

        @Override
        public long append(String tableName,
                           Dataset<Row> rows,
                           List<String> keyColumns,
                           long expectedCount) {
            return delegate.append(tableName, rows, keyColumns, expectedCount);
        }

        @Override
        public long appendDirect(String tableName,
                                 Dataset<Row> rows,
                                 long expectedCount) {
            appendSizes.add(expectedCount);
            List<String> recordTypes = new ArrayList<String>();
            for (Row row : rows.select("record_type").collectAsList()) {
                recordTypes.add(row.getString(0));
            }
            appendRecordTypes.add(recordTypes);
            return delegate.appendDirect(tableName, rows, expectedCount);
        }

        private List<Long> getAppendSizes() {
            return Collections.unmodifiableList(appendSizes);
        }

        private List<String> getLastRecordTypes() {
            assertFalse(appendRecordTypes.isEmpty());
            return appendRecordTypes.get(appendRecordTypes.size() - 1);
        }
    }
}
