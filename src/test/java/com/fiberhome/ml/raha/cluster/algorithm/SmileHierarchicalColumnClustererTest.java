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
 * 验证 Smile 层次聚类实现的可重复性和结果映射。
 */
class SmileHierarchicalColumnClustererTest {

    /** 固定测试时钟。 */
    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);

    @Test
    void shouldClusterDeterministically() {
        FeatureDictionary dictionary = dictionary();
        List<SparseFeatureRow> rows = Arrays.asList(
                row("1", values(1.0d, 0.0d), dictionary),
                row("2", values(2.0d, 0.0d), dictionary),
                row("3", values(0.0d, 1.0d), dictionary),
                row("4", values(0.0d, 2.0d), dictionary));
        SmileHierarchicalColumnClusterer clusterer = new SmileHierarchicalColumnClusterer(
                new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig(
                ClusteringDistanceMetric.COSINE, 2, 100);

        ColumnClusteringResult first = clusterer.cluster("code", dictionary, rows, config, 7L);
        ColumnClusteringResult second = clusterer.cluster("code", dictionary, rows, config, 7L);

        assertEquals(ColumnClusteringStatus.SUCCEEDED, first.getStatus());
        assertEquals(2, first.getEffectiveClusterCount());
        assertEquals(SmileHierarchicalColumnClusterer.ALGORITHM, first.getAlgorithm());
        assertEquals(first.getClusterVersion(), second.getClusterVersion());
        assertEquals(clusterId(first, "1"), clusterId(first, "2"));
        assertEquals(clusterId(first, "3"), clusterId(first, "4"));
        assertFalse(clusterId(first, "1").equals(clusterId(first, "3")));
        assertTrue(first.getAssignments().size() == 4);
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
                HashUtils.md5Hex("value-" + rowId), null,
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
