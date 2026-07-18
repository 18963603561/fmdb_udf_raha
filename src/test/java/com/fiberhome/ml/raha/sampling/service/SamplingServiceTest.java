package com.fiberhome.ml.raha.sampling.service;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ClusteringMetrics;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.adapter.DefaultAnnotationTaskRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTaskStatus;
import com.fiberhome.ml.raha.sampling.domain.TupleSamplingScore;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证低覆盖聚类评分、预算内可复现采样、重复排除和标注任务状态。
 */
class SamplingServiceTest {

    @Test
    void shouldGiveHigherScoreToLessLabeledClusters() {
        List<ClusterAssignment> assignments = assignments();
        List<CellLabel> labels = Arrays.asList(
                label(assignments.get(2).getCellId()),
                label(assignments.get(4).getCellId()));

        List<TupleSamplingScore> scores = new ClusterCoverageScorer().score(
                assignments, labels, Collections.<String>emptySet());

        assertTrue(findScore(scores, "1").getScore() > findScore(scores, "2").getScore());
        assertTrue(findScore(scores, "1").getCoverageScore()
                > findScore(scores, "2").getCoverageScore());
    }

    @Test
    void shouldSampleWithoutReplacementAndReproduceWithSameSeed() {
        List<TupleSamplingScore> scores = new ClusterCoverageScorer().score(
                assignments(), Collections.<CellLabel>emptyList(),
                Collections.<String>emptySet());
        TupleSampler sampler = new TupleSampler();

        List<TupleSamplingScore> first = sampler.select(scores, 3, 20260714L);
        List<TupleSamplingScore> second = sampler.select(scores, 3, 20260714L);

        assertEquals(rowIds(first), rowIds(second));
        assertEquals(3, new LinkedHashSet<String>(rowIds(first)).size());
    }

    @Test
    void shouldPersistTasksAndExcludeLabeledOrAlreadySampledRows() {
        AnnotationTaskRepository repository = new DefaultAnnotationTaskRepository(
                new InMemoryRahaRepository());
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        SamplingService service = new SamplingService(new ClusterCoverageScorer(),
                new TupleSampler(), new SamplingVersioner(), repository, clock);
        SamplingConfig config = new SamplingConfig(2, true, false, 1000L);
        ArtifactVersion version = new ArtifactVersion(
                "config", "snapshot", "sample-stage", 1);
        List<CellLabel> labels = Collections.singletonList(label(assignments().get(0).getCellId()));

        SamplingBatchResult first = service.createTasks("job", 1, clustering(),
                labels, config, 11L, version);
        SamplingBatchResult second = service.createTasks("job", 2, clustering(),
                labels, config, 11L, version);

        assertEquals(2, first.getTasks().size());
        assertEquals(1, second.getTasks().size());
        assertEquals(3, repository.findByJob("job").size());
        assertTrue(first.getTasks().stream().noneMatch(task -> "1".equals(task.getRowId())));
        assertNotEquals(first.getSamplingVersion(), second.getSamplingVersion());
    }

    @Test
    void shouldSupportCompletedExpiredAndCancelledStates() {
        AnnotationTask completed = task("completed", 1000L, 2000L);
        AnnotationTask expired = task("expired", 1000L, 2000L);
        AnnotationTask cancelled = task("cancelled", 1000L, 2000L);

        completed.complete(1500L);
        expired.expire(2000L);
        cancelled.cancel(1200L);

        assertEquals(AnnotationTaskStatus.COMPLETED, completed.getStatus());
        assertEquals(AnnotationTaskStatus.EXPIRED, expired.getStatus());
        assertEquals(AnnotationTaskStatus.CANCELLED, cancelled.getStatus());
        assertThrows(IllegalStateException.class, () -> completed.cancel(1600L));
        assertThrows(IllegalArgumentException.class,
                () -> task("early", 1000L, 2000L).expire(1999L));
    }

    private static AnnotationTask task(String id, long createdAt, long expiresAt) {
        return new AnnotationTask(id, "job", id, 1, 1.0d,
                Collections.singletonMap("code", "cluster-v1|cluster-1"),
                "sampling-v1", createdAt, expiresAt);
    }

    private static CellLabel label(String cellId) {
        return new CellLabel(cellId, 0, LabelSource.HUMAN,
                1.0d, null, null, "tester", 1L);
    }

    private static TupleSamplingScore findScore(List<TupleSamplingScore> scores,
                                                String rowId) {
        for (TupleSamplingScore score : scores) {
            if (score.getRowId().equals(rowId)) {
                return score;
            }
        }
        return null;
    }

    private static List<String> rowIds(List<TupleSamplingScore> scores) {
        List<String> rowIds = new ArrayList<String>();
        for (TupleSamplingScore score : scores) {
            rowIds.add(score.getRowId());
        }
        return rowIds;
    }

    private static ClusteringBatchResult clustering() {
        List<ClusterAssignment> assignments = assignments();
        Map<String, ColumnClusteringResult> results =
                new LinkedHashMap<String, ColumnClusteringResult>();
        for (String column : Arrays.asList("code", "city")) {
            List<ClusterAssignment> columnAssignments = new ArrayList<ClusterAssignment>();
            for (ClusterAssignment assignment : assignments) {
                if (column.equals(assignment.getColumnName())) {
                    columnAssignments.add(assignment);
                }
            }
            results.put(column, new ColumnClusteringResult(column,
                    "hierarchical_average_cosine_v1", ClusteringDistanceMetric.COSINE,
                    2, 2, 11L, "cluster-version-" + column,
                    ColumnClusteringStatus.SUCCEEDED, "聚类完成", columnAssignments, 1L));
        }
        return new ClusteringBatchResult(results,
                new ClusteringMetrics(2, 2, assignments.size(), 0));
    }

    private static List<ClusterAssignment> assignments() {
        List<ClusterAssignment> assignments = new ArrayList<ClusterAssignment>();
        for (String rowId : Arrays.asList("1", "2", "3", "4")) {
            assignments.add(assignment(rowId, "code",
                    "1".equals(rowId) ? "code-unlabeled" : "code-labeled"));
            assignments.add(assignment(rowId, "city",
                    "1".equals(rowId) ? "city-unlabeled" : "city-labeled"));
        }
        return assignments;
    }

    private static ClusterAssignment assignment(String rowId,
                                                String column,
                                                String clusterId) {
        CellCoordinate coordinate = new CellCoordinate(
                "dataset", "snapshot", rowId, column);
        return new ClusterAssignment(coordinate.toCellId(), column, coordinate,
                clusterId, "hierarchical_average_cosine_v1",
                "cluster-version-" + column, 0.0d);
    }
}
