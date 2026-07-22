package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 AUTO 聚类会按单列样本量选择 Smile 精确路径或可扩展近似路径。
 */
class AutoColumnClustererTest {

    /** 固定测试时钟。 */
    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);

    @Test
    void shouldResolveAutoProviderToAutoClusterer() {
        ColumnClusterer clusterer = ClusteringProviderResolver.resolve(
                "AUTO", new ClusterVersioner(), clock);

        assertTrue(clusterer instanceof AutoColumnClusterer);
        assertEquals("AUTO", ClusteringProviderResolver.canonicalProvider("AUTO"));
    }

    @Test
    void shouldUseSmileForRowsWithinExactLimit() {
        AutoColumnClusterer clusterer = new AutoColumnClusterer(
                new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig("AUTO",
                ClusteringDistanceMetric.COSINE, 3, 5);

        ColumnClusteringResult result = clusterer.cluster(
                "code", dictionary(), rows(5), config, 20260722L);

        assertEquals(ColumnClusteringStatus.SUCCEEDED, result.getStatus());
        assertEquals(SmileHierarchicalColumnClusterer.ALGORITHM,
                result.getAlgorithm());
        assertEquals(5, result.getAssignments().size());
    }

    @Test
    void shouldUseScalableForRowsAboveExactLimit() {
        AutoColumnClusterer clusterer = new AutoColumnClusterer(
                new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig("AUTO",
                ClusteringDistanceMetric.COSINE, 3, 5);

        ColumnClusteringResult result = clusterer.cluster(
                "code", dictionary(), rows(12), config, 20260722L);

        assertEquals(ColumnClusteringStatus.SUCCEEDED, result.getStatus());
        assertEquals(ScalableColumnClusterer.APPROXIMATE_ALGORITHM,
                result.getAlgorithm());
        assertEquals(12, result.getAssignments().size());
    }

    private static FeatureDictionary dictionary() {
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(0, "f0",
                FeatureType.NUMERIC, "test", 0.0d));
        definitions.put(1, new FeatureDefinition(1, "f1",
                FeatureType.NUMERIC, "test", 0.0d));
        return new FeatureDictionary("dictionary-v1", "code",
                definitions, 1L);
    }

    private static List<SparseFeatureRow> rows(int count) {
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        for (int index = 0; index < count; index++) {
            CellCoordinate coordinate = new CellCoordinate(
                    "dataset", "snapshot", String.valueOf(index), "code");
            Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
            values.put(index % 2, 1.0d + index % 3);
            rows.add(new SparseFeatureRow(coordinate.toCellId(), "code",
                    coordinate, HashUtils.md5Hex("value-" + index), null,
                    "dictionary-v1", values,
                    Collections.<String, String>emptyMap()));
        }
        return rows;
    }
}
