package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.data.ModelStatus;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证三段评测划分和召回约束下的精度优先阈值选择。
 */
class EvaluationSplitAndThresholdPolicyTest {

    @Test
    void shouldSplitValidationAndTestDeterministicallyWithoutDirectLabels() {
        List<CellLabel> labels = new ArrayList<CellLabel>();
        Set<String> direct = new LinkedHashSet<String>();
        for (int index = 0; index < 100; index++) {
            String cellId = "cell-" + index;
            labels.add(label(cellId, index % 3 == 0 ? 1 : 0));
            if (index < 5) {
                direct.add(cellId);
            }
        }
        EvaluationSplitService service = new EvaluationSplitService();

        EvaluationSplit first = service.split(labels, direct, 5, 0, "split-v1");
        EvaluationSplit second = service.split(labels, direct, 5, 0, "split-v1");

        assertEquals(first.getValidationCellIds(), second.getValidationCellIds());
        assertEquals(first.getTestCellIds(), second.getTestCellIds());
        assertEquals(95, first.getValidationLabels().size()
                + first.getTestLabels().size());
        assertTrue(Collections.disjoint(first.getValidationCellIds(),
                first.getTestCellIds()));
        assertTrue(Collections.disjoint(direct, first.getValidationCellIds()));
        assertTrue(Collections.disjoint(direct, first.getTestCellIds()));
    }

    @Test
    void shouldPreferPrecisionWithoutBreakingRecallFloor() {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
        ModelMetadataRepository repository = new DefaultModelMetadataRepository(
                new InMemoryRahaRepository());
        ThresholdComparisonService service = new ThresholdComparisonService(
                new DetectionEvaluationService(), repository, clock);
        List<CellScore> scores = Arrays.asList(
                new CellScore("p1", 0.90d), new CellScore("p2", 0.60d),
                new CellScore("n1", 0.55d), new CellScore("n2", 0.40d));
        List<CellLabel> labels = Arrays.asList(
                label("p1", 1), label("p2", 1),
                label("n1", 0), label("n2", 0));

        ThresholdComparisonResult result = service.compareAndSave(
                model(), scores, labels, Arrays.asList(0.50d, 0.60d, 0.90d),
                new ThresholdSelectionPolicy(0.70d, 0.05d, 0.50d),
                new ArtifactVersion("config", "snapshot", "threshold", 1));

        assertEquals(0.60d, result.getSelectedThreshold(), 0.0d);
        assertEquals(1.0d, result.getUpdatedModel().getMetrics().get(
                "evaluation.precision"), 0.0d);
        assertEquals(1.0d, result.getUpdatedModel().getMetrics().get(
                "evaluation.recall"), 0.0d);
        assertFalse(result.getUpdatedModel().getMetrics().get(
                "evaluation.constraintFallback") > 0.0d);
    }

    private static CellLabel label(String cellId, int value) {
        return new CellLabel(cellId, value, LabelSource.GROUND_TRUTH,
                1.0d, null, null, "tester", 1L);
    }

    private static RahaColumnModel model() {
        return new RahaColumnModel("model", "version", "dataset",
                "act_dep_time", "schema", ClassifierType.LOGISTIC_REGRESSION,
                "dictionary", "plan", 0.5d, "model-path",
                ModelStatus.CANDIDATE, Collections.<String, Double>emptyMap(), 1L);
    }
}
