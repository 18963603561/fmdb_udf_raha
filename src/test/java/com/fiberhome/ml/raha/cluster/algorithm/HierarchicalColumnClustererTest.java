package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.adapter.DefaultClusterRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.ClusterRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证层次聚类可复现性、余弦分组、成员持久化和异常状态。
 */
class HierarchicalColumnClustererTest {

    /** 固定测试时钟。 */
    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);

    @Test
    void shouldReproduceCosineClustersAndPersistAssignments() {
        FeatureDictionary dictionary = dictionary();
        List<SparseFeatureRow> rows = Arrays.asList(
                row("1", values(1.0d, 0.0d), dictionary),
                row("2", values(2.0d, 0.0d), dictionary),
                row("3", values(0.0d, 1.0d), dictionary),
                row("4", values(0.0d, 2.0d), dictionary));
        HierarchicalColumnClusterer clusterer = new HierarchicalColumnClusterer(
                new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig(
                ClusteringDistanceMetric.COSINE, 2, 100);

        ColumnClusteringResult first = clusterer.cluster("code", dictionary, rows, config, 7L);
        ColumnClusteringResult second = clusterer.cluster("code", dictionary, rows, config, 7L);

        assertEquals(ColumnClusteringStatus.SUCCEEDED, first.getStatus());
        assertEquals(2, first.getEffectiveClusterCount());
        assertEquals(first.getClusterVersion(), second.getClusterVersion());
        assertEquals(clusterId(first, "1"), clusterId(first, "2"));
        assertEquals(clusterId(first, "3"), clusterId(first, "4"));
        assertFalse(clusterId(first, "1").equals(clusterId(first, "3")));

        ClusterRepository repository = new DefaultClusterRepository(
                new InMemoryRahaRepository());
        repository.save("job", first,
                new ArtifactVersion("config", "snapshot", "cluster-stage", 1), 1L);
        assertEquals(4, repository.findAssignments(
                "job", "code", first.getClusterVersion()).size());
        assertTrue(repository.findResult(
                "job", "code", first.getClusterVersion()).isPresent());
    }

    @Test
    void shouldExplainEmptyFeatureAndSingleSampleColumns() {
        FeatureDictionary emptyDictionary = new FeatureDictionary(
                "empty-v1", "code", Collections.<Integer, FeatureDefinition>emptyMap(), 1L);
        SparseFeatureRow emptyRow = row("1", Collections.<Integer, Double>emptyMap(),
                emptyDictionary);
        HierarchicalColumnClusterer clusterer = new HierarchicalColumnClusterer(
                new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig(
                ClusteringDistanceMetric.COSINE, 2, 100);

        ColumnClusteringResult single = clusterer.cluster("code", emptyDictionary,
                Collections.singletonList(emptyRow), config, 7L);
        ColumnClusteringResult emptyFeatures = clusterer.cluster("code", emptyDictionary,
                Arrays.asList(emptyRow,
                        row("2", Collections.<Integer, Double>emptyMap(), emptyDictionary)),
                config, 7L);

        assertEquals(ColumnClusteringStatus.SINGLE_SAMPLE, single.getStatus());
        assertEquals(1, single.getAssignments().size());
        assertEquals(ColumnClusteringStatus.EMPTY_FEATURES, emptyFeatures.getStatus());
        assertTrue(emptyFeatures.getAssignments().isEmpty());
    }

    @Test
    void shouldReturnInputLimitStatusInsteadOfFailingColumn() {
        FeatureDictionary dictionary = dictionary();
        HierarchicalColumnClusterer clusterer = new HierarchicalColumnClusterer(
                new ClusterVersioner(), clock);

        ColumnClusteringResult result = clusterer.cluster("code", dictionary,
                Arrays.asList(row("1", values(1.0d, 0.0d), dictionary),
                        row("2", values(0.0d, 1.0d), dictionary)),
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 2, 1), 7L);

        assertEquals(ColumnClusteringStatus.INPUT_LIMIT_EXCEEDED, result.getStatus());
        assertTrue(result.getMessage().contains("上限"));
    }

    @Test
    void shouldIsolateUnexpectedColumnFailureWithAlgorithmMetadata() {
        FeatureDictionary dictionary = dictionary();
        SparseFeatureRow row = row("1", values(1.0d, 0.0d), dictionary);
        ColumnClusterer failingClusterer = new ColumnClusterer() {
            @Override
            public String getAlgorithm() {
                return "test-failing-clusterer";
            }

            @Override
            public ColumnClusteringResult cluster(String columnName,
                                                  FeatureDictionary currentDictionary,
                                                  List<SparseFeatureRow> currentRows,
                                                  ClusteringConfig config,
                                                  long randomSeed) {
                throw new IllegalStateException("模拟聚类失败");
            }
        };
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        ColumnClusteringService service = new ColumnClusteringService(
                failingClusterer, new DefaultClusterRepository(storage), clock);
        FeatureAssemblyResult features = new FeatureAssemblyResult(
                Collections.singletonMap("code", dictionary),
                Collections.singletonList(row), new FeatureAssemblyMetrics(1, 2, 1, 1));

        ClusteringBatchResult batch = service.clusterAndSave("job", features,
                new ClusteringConfig(ClusteringDistanceMetric.COSINE, 2, 100), 7L,
                new ArtifactVersion("config", "snapshot", "cluster-stage", 1));

        ColumnClusteringResult result = batch.getResults().get("code");
        assertEquals(ColumnClusteringStatus.FAILED, result.getStatus());
        assertEquals("test-failing-clusterer", result.getAlgorithm());
        assertTrue(result.getMessage().contains("IllegalStateException"));
        assertTrue(new DefaultClusterRepository(storage).findResult(
                "job", "code", result.getClusterVersion()).isPresent());
    }

    private static String clusterId(ColumnClusteringResult result, String rowId) {
        for (ClusterAssignment assignment : result.getAssignments()) {
            if (assignment.getCoordinate().getRowId().equals(rowId)) {
                return assignment.getClusterId();
            }
        }
        return null;
    }

    private static FeatureDictionary dictionary() {
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(
                0, "feature-a", FeatureType.NUMERIC, "test", 0.0d));
        definitions.put(1, new FeatureDefinition(
                1, "feature-b", FeatureType.NUMERIC, "test", 0.0d));
        return new FeatureDictionary("dictionary-v1", "code", definitions, 1L);
    }

    private static SparseFeatureRow row(String rowId,
                                        Map<Integer, Double> values,
                                        FeatureDictionary dictionary) {
        CellCoordinate coordinate = new CellCoordinate(
                "dataset", "snapshot", rowId, "code");
        return new SparseFeatureRow(coordinate.toCellId(), "code", coordinate,
                HashUtils.sha256Hex("value-" + rowId), null,
                dictionary.getVersion(), values, Collections.<String, String>emptyMap());
    }

    private static Map<Integer, Double> values(double first, double second) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        if (first != 0.0d) {
            values.put(0, first);
        }
        if (second != 0.0d) {
            values.put(1, second);
        }
        return values;
    }
}
