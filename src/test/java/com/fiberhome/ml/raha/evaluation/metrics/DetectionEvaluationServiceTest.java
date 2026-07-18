package com.fiberhome.ml.raha.evaluation.metrics;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.evaluation.threshold.ThresholdComparisonResult;
import com.fiberhome.ml.raha.evaluation.threshold.ThresholdComparisonService;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.FileColumnModelStore;
import com.fiberhome.ml.raha.model.release.ColumnModelCompatibilityValidator;
import com.fiberhome.ml.raha.model.release.ModelReleaseManager;
import com.fiberhome.ml.raha.model.release.PublishedColumnModelLoader;
import com.fiberhome.ml.raha.repository.adapter.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证检测指标数学、平均精确率、阈值选优和发布阈值生效。
 */
class DetectionEvaluationServiceTest {

    /** JUnit 提供的隔离模型目录。 */
    @TempDir
    Path temporaryDirectory;

    @Test
    void shouldCalculateConfusionMetricsAndAveragePrecision() {
        List<CellLabel> truth = detectionTruth(1, 1, 0, 0);
        List<DetectionResult> detections = Arrays.asList(
                detection("c1", 0.9d, true),
                detection("c2", 0.4d, false),
                detection("c3", 0.8d, true),
                detection("c4", 0.1d, false));

        DetectionEvaluationMetrics metrics =
                new DetectionEvaluationService().evaluate(detections, truth);

        assertEquals(1L, metrics.getTruePositive());
        assertEquals(1L, metrics.getFalsePositive());
        assertEquals(1L, metrics.getFalseNegative());
        assertEquals(1L, metrics.getTrueNegative());
        assertEquals(0.5d, metrics.getPrecision(), 0.000001d);
        assertEquals(0.5d, metrics.getRecall(), 0.000001d);
        assertEquals(0.5d, metrics.getF1(), 0.000001d);
        assertEquals(5.0d / 6.0d, metrics.getAveragePrecision(), 0.000001d);
    }

    @Test
    void shouldSelectThresholdPersistMetricsAndUseItAfterPublish() {
        String modelVersion = HashUtils.sha256Hex("threshold-model");
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        coefficients.put(0, 1.0d);
        ColumnModelArtifact artifact = new ColumnModelArtifact("raha-code",
                modelVersion, "code", ClassifierType.WEIGHTED_RULE,
                "dictionary-v1", 1, 0.2d, 0.0d, coefficients, "test-weighted");
        FileColumnModelStore store = new FileColumnModelStore(temporaryDirectory);
        String path = store.save(artifact);
        RahaColumnModel candidate = new RahaColumnModel("raha-code", modelVersion,
                "dataset", "code", "schema-v1", ClassifierType.WEIGHTED_RULE,
                "dictionary-v1", "plan-v1", 0.2d, path,
                ModelStatus.CANDIDATE, Collections.<String, Double>emptyMap(), 1000L);
        ModelMetadataRepository repository = new DefaultModelMetadataRepository(
                new InMemoryRahaRepository());
        repository.saveAll(Collections.singletonList(candidate), version("initial"), 1000L);
        List<CellScore> scores = Arrays.asList(
                new CellScore("c1", 0.9d), new CellScore("c2", 0.6d),
                new CellScore("c3", 0.4d), new CellScore("c4", 0.1d));

        ThresholdComparisonResult result = new ThresholdComparisonService(
                new DetectionEvaluationService(), repository,
                fixedClock(2000L)).compareAndSave(candidate, scores,
                truth(1, 1, 0, 0), Arrays.asList(0.3d, 0.5d, 0.8d),
                version("threshold"));

        assertEquals(0.5d, result.getSelectedThreshold(), 0.000001d);
        assertEquals(1.0d, result.getUpdatedModel().getMetrics()
                .get("evaluation.f1"), 0.000001d);
        assertEquals(0.5d, repository.find(
                "dataset", "code", modelVersion).get().getThreshold(), 0.000001d);
        new ModelReleaseManager(repository, fixedClock(3000L)).publish(
                "dataset", "code", modelVersion, version("publish"));
        ColumnModelArtifact published = new PublishedColumnModelLoader(repository,
                store, new ColumnModelCompatibilityValidator()).load(
                "dataset", "code", "schema-v1", "dictionary-v1", "plan-v1");
        assertEquals(0.5d, published.getThreshold(), 0.000001d);
        assertThrows(IllegalStateException.class,
                () -> result.getUpdatedModel().withStatus(ModelStatus.PUBLISHED)
                        .withEvaluation(0.4d, Collections.<String, Double>emptyMap()));
    }

    private static List<CellLabel> truth(int first,
                                         int second,
                                         int third,
                                         int fourth) {
        return Arrays.asList(
                truthLabel("c1", first, 1000L),
                truthLabel("c2", second, 1001L),
                truthLabel("c3", third, 1002L),
                truthLabel("c4", fourth, 1003L));
    }

    private static List<CellLabel> detectionTruth(int first,
                                                  int second,
                                                  int third,
                                                  int fourth) {
        return Arrays.asList(
                truthLabel(coordinate("c1").toCellId(), first, 1000L),
                truthLabel(coordinate("c2").toCellId(), second, 1001L),
                truthLabel(coordinate("c3").toCellId(), third, 1002L),
                truthLabel(coordinate("c4").toCellId(), fourth, 1003L));
    }

    private static CellLabel truthLabel(String cellId, int label, long createdAt) {
        return new CellLabel(cellId, label, LabelSource.GROUND_TRUTH,
                1.0d, null, null, "truth", createdAt);
    }

    private static DetectionResult detection(String cellId,
                                             double score,
                                             boolean error) {
        CellCoordinate coordinate = coordinate(cellId);
        return new DetectionResult("job", coordinate, HashUtils.sha256Hex(cellId),
                null, error, score, 0.5d, Collections.<String>emptyList(),
                Collections.<String, String>emptyMap(), "model", "model-v1",
                "dictionary-v1", 1000L);
    }

    private static CellCoordinate coordinate(String rowId) {
        return new CellCoordinate("dataset", "snapshot", rowId, "code");
    }

    private static ArtifactVersion version(String stageId) {
        return new ArtifactVersion("config-v1", "snapshot-v1", stageId, 1);
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
