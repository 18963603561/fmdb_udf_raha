package com.fiberhome.ml.raha.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.algorithm.HierarchicalColumnClusterer;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.config.dto.FeatureConfig;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssembler;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.adapter.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultFeatureRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * 验证批内并行特征和聚类与串行路径保持确定性一致。
 */
class FeatureAndClusteringParallelIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldKeepParallelFeaturesAndClustersConsistentWithSerialPath() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1784730000000L),
                ZoneOffset.UTC);
        RahaDataset baseDataset = dataset();
        RahaDataset dataset = baseDataset.withProfiles(
                new ColumnProfiler().profileDetectable(baseDataset));
        ArtifactVersion featureVersion = new ArtifactVersion(
                "config-v1", "snapshot-v1", "feature-stage", 1);
        FeatureService serialFeatureService = featureService(clock);
        FeatureService parallelFeatureService = featureService(clock);

        FeatureAssemblyResult serial = serialFeatureService.assembleAndSave(
                "serial-feature", dataset, Collections.emptyList(),
                Collections.emptyList(), FeatureConfig.defaults(),
                featureVersion);
        FeatureAssemblyResult parallel = parallelFeatureService
                .assembleAndSaveParallel("parallel-feature", dataset,
                        Collections.emptyList(), Collections.emptyList(),
                        FeatureConfig.defaults(), featureVersion, 2, 60000L);

        assertEquals(serial.getDictionaries().keySet(),
                parallel.getDictionaries().keySet());
        assertEquals(serial.getRows().size(), parallel.getRows().size());
        assertEquals(serial.getMetrics().getCellCount(),
                parallel.getMetrics().getCellCount());
        assertEquals(dictionaryVersions(serial), dictionaryVersions(parallel));
        assertEquals(cellIds(serial.getRows()), cellIds(parallel.getRows()));

        ClusteringConfig config = new ClusteringConfig(
                ClusteringDistanceMetric.COSINE, 2, 100);
        ArtifactVersion clusterVersion = new ArtifactVersion(
                "config-v1", "snapshot-v1", "cluster-stage", 1);
        ClusteringBatchResult serialClusters = clusteringService(clock)
                .clusterAndSave("serial-cluster", serial, config, 20260722L,
                        clusterVersion);
        ClusteringBatchResult parallelClusters = clusteringService(clock)
                .clusterAndSaveParallel("parallel-cluster", parallel, config,
                        20260722L, clusterVersion, 2, 60000L);

        assertEquals(serialClusters.getResults().keySet(),
                parallelClusters.getResults().keySet());
        assertEquals(serialClusters.getMetrics().getAssignmentCount(),
                parallelClusters.getMetrics().getAssignmentCount());
        assertEquals(serialClusters.getMetrics().getClusteredColumnCount(),
                parallelClusters.getMetrics().getClusteredColumnCount());
    }

    private static FeatureService featureService(Clock clock) {
        return new FeatureService(new FeatureAssembler(
                new FeatureDictionaryVersioner(), clock),
                new DefaultFeatureRepository(new InMemoryRahaRepository()),
                clock);
    }

    private static ColumnClusteringService clusteringService(Clock clock) {
        return new ColumnClusteringService(
                new HierarchicalColumnClusterer(new ClusterVersioner(), clock),
                new DefaultClusterRepository(new InMemoryRahaRepository()),
                clock);
    }

    private static List<String> dictionaryVersions(
            FeatureAssemblyResult result) {
        List<String> versions = new ArrayList<String>();
        result.getDictionaries().values().forEach(
                dictionary -> versions.add(dictionary.getVersion()));
        return versions;
    }

    private static List<String> cellIds(List<SparseFeatureRow> rows) {
        List<String> ids = new ArrayList<String>();
        for (SparseFeatureRow row : rows) {
            ids.add(row.getCellId());
        }
        return ids;
    }

    private static RahaDataset dataset() {
        List<Row> rows = Arrays.asList(
                RowFactory.create("1", "A", "X"),
                RowFactory.create("2", "A", "X"),
                RowFactory.create("3", "B", "Y"),
                RowFactory.create("4", "C", "Z"));
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("c1", DataTypes.StringType, true)
                .add("c2", DataTypes.StringType, true);
        return new RahaDataset("dataset", "snapshot-v1", "wide-table", "id",
                Arrays.asList(
                        new ColumnMetadata("id", 0, "string", false,
                                false, false),
                        new ColumnMetadata("c1", 1, "string", true,
                                true, false),
                        new ColumnMetadata("c2", 2, "string", true,
                                true, false)),
                SparkTestSession.get().createDataFrame(rows, schema),
                "schema-v1", Collections.emptyMap());
    }
}
