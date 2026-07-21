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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证超过精确样本上限后自动切换到可复现的大样本聚类路径。
 */
class ScalableColumnClustererTest {

    @Test
    void shouldClusterAllLargeInputRowsDeterministically() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        ScalableColumnClusterer clusterer = new ScalableColumnClusterer(
                new ClusterVersioner(), clock);
        ClusteringConfig config = new ClusteringConfig(
                ClusteringDistanceMetric.COSINE, 3, 5);

        ColumnClusteringResult first = clusterer.cluster(
                "code", dictionary(), rows(), config, 20260715L);
        ColumnClusteringResult second = clusterer.cluster(
                "code", dictionary(), rows(), config, 20260715L);

        assertEquals(ColumnClusteringStatus.SUCCEEDED, first.getStatus());
        assertEquals(ScalableColumnClusterer.APPROXIMATE_ALGORITHM,
                first.getAlgorithm());
        assertEquals(12, first.getAssignments().size());
        assertTrue(first.getEffectiveClusterCount() > 0);
        assertEquals(first.getClusterVersion(), second.getClusterVersion());
        assertEquals(memberships(first), memberships(second));
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

    private static List<SparseFeatureRow> rows() {
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        for (int index = 0; index < 12; index++) {
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

    private static Map<String, String> memberships(ColumnClusteringResult result) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        for (ClusterAssignment assignment : result.getAssignments()) {
            values.put(assignment.getCellId(), assignment.getClusterId());
        }
        return values;
    }
}
