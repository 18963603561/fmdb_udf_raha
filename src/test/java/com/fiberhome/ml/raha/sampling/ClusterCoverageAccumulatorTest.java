package com.fiberhome.ml.raha.sampling;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ClusteringMetrics;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.sampling.domain.TupleSamplingScore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** 验证分批累积与一次性聚类覆盖评分保持一致。 */
class ClusterCoverageAccumulatorTest {

    @Test
    void shouldMatchFullAssignmentCoverageAcrossColumnBatches() {
        List<ClusterAssignment> all = new ArrayList<ClusterAssignment>();
        ClusterCoverageAccumulator accumulator =
                new ClusterCoverageAccumulator(Collections.emptyList(), 20.0d);
        for (String column : new String[] {"c01", "c02"}) {
            ClusteringBatchResult batch = batch(column);
            accumulator.addBatch(batch);
            all.addAll(batch.getResults().get(column).getAssignments());
        }

        List<TupleSamplingScore> expected = new ClusterCoverageScorer().score(
                all, Collections.emptyList(), Collections.emptySet(), 20.0d);
        List<TupleSamplingScore> actual = accumulator.scores();

        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.get(index).getRowId(), actual.get(index).getRowId());
            assertEquals(expected.get(index).getScore(), actual.get(index).getScore());
            assertEquals(Collections.emptyMap(),
                    actual.get(index).getCoveredClusters());
            assertEquals(expected.get(index).getCoveredClusters(),
                    accumulator.coveredClusters(actual.get(index).getRowId()));
        }
        assertEquals(2, accumulator.getClusterVersions().size());
    }

    private static ClusteringBatchResult batch(String column) {
        List<ClusterAssignment> assignments =
                new ArrayList<ClusterAssignment>();
        for (String rowId : new String[] {"1", "2"}) {
            CellCoordinate coordinate = new CellCoordinate("dataset-1",
                    "snapshot-1", rowId, column);
            assignments.add(new ClusterAssignment(coordinate.toCellId(),
                    column, coordinate, "cluster-" + rowId,
                    "test-cluster", "version-" + column, null));
        }
        ColumnClusteringResult result = new ColumnClusteringResult(column,
                "test-cluster", ClusteringDistanceMetric.COSINE, 2, 2,
                7L, "version-" + column,
                ColumnClusteringStatus.SUCCEEDED, "完成", assignments, 1L);
        Map<String, ColumnClusteringResult> results =
                new LinkedHashMap<String, ColumnClusteringResult>();
        results.put(column, result);
        return new ClusteringBatchResult(results,
                new ClusteringMetrics(1L, 1L, 2L, 0L));
    }
}
