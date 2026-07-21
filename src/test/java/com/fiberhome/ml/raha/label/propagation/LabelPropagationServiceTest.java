package com.fiberhome.ml.raha.label.propagation;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.adapter.DefaultCellLabelRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.adapter.StoredCellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.CellLabelRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证标签仓储、同质传播、多数传播、冲突统计和传播权重。
 */
class LabelPropagationServiceTest {

    /** 固定仓储业务版本。 */
    private final ArtifactVersion artifactVersion =
            new ArtifactVersion("config-v1", "snapshot-v1", "label-stage", 1);
    /** 标签仓储。 */
    private CellLabelRepository repository;
    /** 待测试标签传播服务。 */
    private LabelPropagationService service;

    @BeforeEach
    void setUp() {
        repository = new DefaultCellLabelRepository(new InMemoryRahaRepository());
        service = new LabelPropagationService(repository,
                Clock.fixed(Instant.ofEpochMilli(2000L), ZoneOffset.UTC));
    }

    @Test
    void shouldPropagateHomogeneousLabelsAndPreserveTraceability() {
        List<ClusterAssignment> assignments = assignments(4);
        CellLabel direct = directLabel(assignments.get(0).getCellId(), 1, 1000L);

        LabelPropagationResult result = service.propagateAndSave("job-homogeneous",
                assignments, Collections.singletonList(direct),
                LabelPropagationMethod.HOMOGENEITY,
                LabelPropagationConfig.defaults(), artifactVersion);

        assertEquals(4, result.getLabels().size());
        assertSame(direct, result.getLabels().get(0));
        assertEquals(3L, result.getMetrics().getPropagatedLabelCount());
        CellLabel propagated = findLabel(result.getLabels(), assignments.get(1).getCellId());
        assertNotNull(propagated);
        assertEquals(LabelSource.PROPAGATED, propagated.getLabelSource());
        assertEquals(LabelPropagationMethod.HOMOGENEITY,
                propagated.getPropagationMethod());
        assertEquals("cluster-v1", propagated.getClusterVersion());
        assertEquals(1.0d, propagated.getConfidence(), 0.000001d);
        assertTrue(propagated.getSampleWeight() < direct.getSampleWeight());
        assertTrue(propagated.getSourceLabelId().matches("[0-9a-f]{32}"));

        List<StoredCellLabel> stored = repository.findByCell(
                "job-homogeneous", assignments.get(1).getCellId());
        assertEquals(1, stored.size());
        assertEquals(artifactVersion, stored.get(0).getArtifactVersion());
        assertEquals(propagated.getLabelId(), stored.get(0).getLabel().getLabelId());
        ClusterPropagationSummary summary = result.getSummaries().get(0);
        assertEquals(ClusterPropagationStatus.PROPAGATED, summary.getStatus());
        assertEquals(1, summary.getDirectLabelCount());
        assertEquals(3, summary.getPropagatedLabelCount());
    }

    @Test
    void shouldRejectHomogeneousPropagationWhenDirectLabelsConflict() {
        List<ClusterAssignment> assignments = assignments(3);
        List<CellLabel> labels = Arrays.asList(
                directLabel(assignments.get(0).getCellId(), 1, 1000L),
                directLabel(assignments.get(1).getCellId(), 0, 1001L));

        LabelPropagationResult result = service.propagateAndSave("job-conflict",
                assignments, labels, LabelPropagationMethod.HOMOGENEITY,
                LabelPropagationConfig.defaults(), artifactVersion);

        assertEquals(2, result.getLabels().size());
        assertEquals(0L, result.getMetrics().getPropagatedLabelCount());
        assertEquals(1L, result.getMetrics().getConflictClusterCount());
        ClusterPropagationSummary summary = result.getSummaries().get(0);
        assertEquals(ClusterPropagationStatus.CONFLICT, summary.getStatus());
        assertEquals(1, summary.getConflictCount());
        assertFalse(repository.findByCell(
                "job-conflict", assignments.get(2).getCellId()).iterator().hasNext());
    }

    @Test
    void shouldPropagateMajorityAndRecordRatioConflictAndWeight() {
        List<ClusterAssignment> assignments = assignments(4);
        List<CellLabel> labels = Arrays.asList(
                directLabel(assignments.get(0).getCellId(), 1, 1000L),
                directLabel(assignments.get(1).getCellId(), 1, 1001L),
                directLabel(assignments.get(2).getCellId(), 0, 1002L));

        LabelPropagationResult result = service.propagateAndSave("job-majority",
                assignments, labels, LabelPropagationMethod.MAJORITY,
                new LabelPropagationConfig(0.5d, 0.5d), artifactVersion);

        CellLabel propagated = findLabel(result.getLabels(), assignments.get(3).getCellId());
        assertNotNull(propagated);
        assertEquals(1, propagated.getLabel());
        assertEquals(2.0d / 3.0d, propagated.getMajorityRatio(), 0.000001d);
        assertEquals(1, propagated.getConflictCount());
        assertTrue(propagated.getSampleWeight() < labels.get(0).getSampleWeight());
        ClusterPropagationSummary summary = result.getSummaries().get(0);
        assertEquals(ClusterPropagationStatus.PROPAGATED, summary.getStatus());
        assertEquals(2.0d / 3.0d, summary.getMajorityRatio(), 0.000001d);
        assertEquals(1, summary.getConflictCount());
    }

    @Test
    void shouldRecordNoLabelClusterWithoutCreatingLabels() {
        LabelPropagationResult result = service.propagateAndSave("job-no-label",
                assignments(2), Collections.<CellLabel>emptyList(),
                LabelPropagationMethod.MAJORITY,
                LabelPropagationConfig.defaults(), artifactVersion);

        assertTrue(result.getLabels().isEmpty());
        assertEquals(1L, result.getMetrics().getUnlabeledClusterCount());
        assertEquals(ClusterPropagationStatus.NO_LABELS,
                result.getSummaries().get(0).getStatus());
        assertEquals(1, repository.findPropagationSummaries("job-no-label").size());
    }

    private static List<ClusterAssignment> assignments(int count) {
        ClusterAssignment[] assignments = new ClusterAssignment[count];
        for (int index = 0; index < count; index++) {
            String rowId = String.valueOf(index + 1);
            CellCoordinate coordinate = new CellCoordinate(
                    "dataset", "snapshot", rowId, "code");
            assignments[index] = new ClusterAssignment(coordinate.toCellId(), "code",
                    coordinate, "cluster-a", "hierarchical", "cluster-v1", 0.0d);
        }
        return Arrays.asList(assignments);
    }

    private static CellLabel directLabel(String cellId, int label, long createdAt) {
        return new CellLabel(cellId, label, LabelSource.HUMAN,
                1.0d, null, null, "tester", createdAt);
    }

    private static CellLabel findLabel(List<CellLabel> labels, String cellId) {
        for (CellLabel label : labels) {
            if (label.getCellId().equals(cellId)) {
                return label;
            }
        }
        return null;
    }
}
