package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 Spark KMeans 列聚类实现和 AUTO 大样本路由。
 */
class SparkKMeansColumnClustererIntegrationTest {

    /** 固定测试时钟。 */
    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldClusterDeterministicallyWithCosineKMeans() {
        FeatureDictionary dictionary = dictionary();
        List<SparseFeatureRow> rows = Arrays.asList(
                row("1", values(1.0d, 0.0d), dictionary),
                row("2", values(2.0d, 0.0d), dictionary),
                row("3", values(0.0d, 1.0d), dictionary),
                row("4", values(0.0d, 2.0d), dictionary));
        SparkKMeansColumnClusterer clusterer = new SparkKMeansColumnClusterer(
                SparkTestSession.get(), new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig(
                "SparkKMeansColumnClusterer", ClusteringDistanceMetric.COSINE, 2, 2);

        ColumnClusteringResult first = clusterer.cluster("code", dictionary, rows, config, 7L);
        ColumnClusteringResult second = clusterer.cluster("code", dictionary, rows, config, 7L);

        assertEquals(ColumnClusteringStatus.SUCCEEDED, first.getStatus());
        assertEquals(SparkKMeansColumnClusterer.ALGORITHM, first.getAlgorithm());
        assertEquals(2, first.getEffectiveClusterCount());
        assertEquals(first.getClusterVersion(), second.getClusterVersion());
        assertEquals(clusterId(first, "1"), clusterId(first, "2"));
        assertEquals(clusterId(first, "3"), clusterId(first, "4"));
        assertFalse(clusterId(first, "1").equals(clusterId(first, "3")));
    }

    @Test
    void shouldRouteAutoLargeSampleToSparkKMeans() {
        FeatureDictionary dictionary = dictionary();
        AutoColumnClusterer clusterer = new AutoColumnClusterer(
                new ClusterVersioner(), clock, SparkTestSession.get());
        ClusteringConfig config = new ClusteringConfig(
                "AUTO", ClusteringDistanceMetric.COSINE, 2, 2);

        ColumnClusteringResult result = clusterer.cluster(
                "code", dictionary, rows(dictionary), config, 20260722L);

        assertFalse(clusterer.supportsLocalColumnParallelism());
        assertEquals(ColumnClusteringStatus.SUCCEEDED, result.getStatus());
        assertEquals(SparkKMeansColumnClusterer.ALGORITHM, result.getAlgorithm());
    }

    @Test
    void shouldKeepSmileForAutoSmallSample() {
        FeatureDictionary dictionary = dictionary();
        AutoColumnClusterer clusterer = new AutoColumnClusterer(
                new ClusterVersioner(), clock, SparkTestSession.get());
        ClusteringConfig config = new ClusteringConfig(
                "AUTO", ClusteringDistanceMetric.COSINE, 2, 10);

        ColumnClusteringResult result = clusterer.cluster(
                "code", dictionary, rows(dictionary), config, 20260722L);

        assertEquals(ColumnClusteringStatus.SUCCEEDED, result.getStatus());
        assertEquals(SmileHierarchicalColumnClusterer.ALGORITHM, result.getAlgorithm());
    }

    @Test
    void shouldResolveSparkKMeansProviderWithSparkSession() {
        ColumnClusterer clusterer = ClusteringProviderResolver.resolve(
                "KMeans", new ClusterVersioner(), clock, SparkTestSession.get());

        assertTrue(clusterer instanceof SparkKMeansColumnClusterer);
        assertEquals("SparkKMeansColumnClusterer",
                ClusteringProviderResolver.canonicalProvider("SparkKMeans"));
    }

    @Test
    void shouldHandleEmptySingleAndAllZeroInputs() {
        FeatureDictionary dictionary = dictionary();
        SparkKMeansColumnClusterer clusterer = new SparkKMeansColumnClusterer(
                SparkTestSession.get(), new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig(
                "SparkKMeansColumnClusterer", ClusteringDistanceMetric.COSINE, 2, 2);

        assertEquals(ColumnClusteringStatus.EMPTY_INPUT,
                clusterer.cluster("code", dictionary,
                        Collections.<SparseFeatureRow>emptyList(), config, 7L).getStatus());
        assertEquals(ColumnClusteringStatus.SINGLE_SAMPLE,
                clusterer.cluster("code", dictionary,
                        Collections.singletonList(row("1", values(1.0d, 0.0d), dictionary)),
                        config, 7L).getStatus());
        assertEquals(ColumnClusteringStatus.EMPTY_FEATURES,
                clusterer.cluster("code", dictionary,
                        Arrays.asList(row("1", values(0.0d, 0.0d), dictionary),
                                row("2", values(0.0d, 0.0d), dictionary)),
                        config, 7L).getStatus());
    }

    private static String clusterId(ColumnClusteringResult result, String rowId) {
        for (ClusterAssignment assignment : result.getAssignments()) {
            if (assignment.getCoordinate().getRowId().equals(rowId)) {
                return assignment.getClusterId();
            }
        }
        return null;
    }

    private static List<SparseFeatureRow> rows(FeatureDictionary dictionary) {
        return Arrays.asList(
                row("1", values(1.0d, 0.0d), dictionary),
                row("2", values(2.0d, 0.0d), dictionary),
                row("3", values(0.0d, 1.0d), dictionary),
                row("4", values(0.0d, 2.0d), dictionary));
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
                HashUtils.md5Hex("value-" + rowId), null,
                dictionary.getVersion(), values, Collections.<String, String>emptyMap());
    }

    private static Map<Integer, Double> values(double first, double second) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        values.put(0, first);
        values.put(1, second);
        return values;
    }
}
