package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.ModelPersistenceContext;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.model.FileColumnModelStore;
import com.fiberhome.ml.raha.model.prediction.ColumnModelPredictor;
import com.fiberhome.ml.raha.repository.adapter.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证模型文件重启加载、依赖版本追溯、发布隔离、兼容校验和历史版本回滚。
 */
class ModelLifecycleIntegrationTest {

    /** JUnit 提供的隔离模型目录。 */
    @TempDir
    Path temporaryDirectory;

    @Test
    void shouldReloadModelAfterStoreRestartAndKeepPredictionStable() {
        ColumnModelArtifact artifact = artifact("first", 0.25d, 1.5d);
        FileColumnModelStore firstStore = new FileColumnModelStore(temporaryDirectory);
        String modelPath = firstStore.save(artifact, persistenceContext());
        SparseFeatureRow row = row(2.0d);
        double expected = new ColumnModelPredictor().predict(
                artifact, Collections.singletonList(row)).get(0).getScore();

        FileColumnModelStore restartedStore = new FileColumnModelStore(temporaryDirectory);
        ColumnModelArtifact reloaded = restartedStore.load(modelPath);
        double actual = new ColumnModelPredictor().predict(
                reloaded, Collections.singletonList(row)).get(0).getScore();

        assertEquals(artifact.getModelVersion(), reloaded.getModelVersion());
        assertEquals(expected, actual, 0.0000001d);
        assertEquals(artifact.getClassifierType(), reloaded.getClassifierType());
    }

    @Test
    void shouldOnlyLoadPublishedCompatibleModelAndRollbackHistoricalRelease() {
        FileColumnModelStore store = new FileColumnModelStore(temporaryDirectory);
        ColumnModelArtifact firstArtifact = artifact("first", -0.5d, 1.0d);
        ColumnModelArtifact secondArtifact = artifact("second", 0.5d, 2.0d);
        ColumnModelArtifact neverPublishedArtifact = artifact("never", 0.0d, 0.5d);
        RahaColumnModel first = metadata(firstArtifact,
                store.save(firstArtifact, persistenceContext()), 1000L);
        RahaColumnModel neverPublished = metadata(neverPublishedArtifact,
                store.save(neverPublishedArtifact, persistenceContext()), 1500L);
        RahaColumnModel second = metadata(secondArtifact,
                store.save(secondArtifact, persistenceContext()), 2000L);
        ModelMetadataRepository repository = new DefaultModelMetadataRepository(
                new InMemoryRahaRepository());
        PublishedColumnModelLoader loader = new PublishedColumnModelLoader(repository,
                store, new ColumnModelCompatibilityValidator());

        ModelReleaseManager firstManager = releaseManager(repository, 3000L);
        firstManager.markCandidate(first, version("candidate-first"));
        assertThrows(IllegalStateException.class,
                () -> loader.load("dataset", "code", "schema-v1",
                        "dictionary-v1", "plan-v1"));
        firstManager.publish("dataset", "code", first.getModelVersion(),
                version("publish-first"));
        assertEquals(first.getModelVersion(), loader.load("dataset", "code",
                "schema-v1", "dictionary-v1", "plan-v1").getModelVersion());

        ModelReleaseManager secondManager = releaseManager(repository, 4000L);
        secondManager.markCandidate(neverPublished, version("candidate-never"));
        secondManager.disable("dataset", "code", neverPublished.getModelVersion(),
                version("disable-never"));
        secondManager.markCandidate(second, version("candidate-second"));
        secondManager.publish("dataset", "code", second.getModelVersion(),
                version("publish-second"));
        assertEquals(first.getModelVersion(), loader.load(
                first.getModelSetVersion(), "dataset", "code", "schema-v1",
                "dictionary-v1", "plan-v1").getModelVersion());

        RahaColumnModel storedSecond = repository.find(
                "dataset", "code", second.getModelVersion()).get();
        assertEquals("schema-v1", storedSecond.getSchemaHash());
        assertEquals("dictionary-v1", storedSecond.getFeatureDictionaryVersion());
        assertEquals("plan-v1", storedSecond.getStrategyPlanVersion());
        assertEquals(ModelStatus.PUBLISHED, storedSecond.getStatus());
        assertThrows(IllegalStateException.class,
                () -> loader.load("dataset", "code", "schema-v2",
                        "dictionary-v1", "plan-v1"));
        assertThrows(IllegalStateException.class,
                () -> loader.load("dataset", "code", "schema-v1",
                        "dictionary-v2", "plan-v1"));
        assertThrows(IllegalStateException.class,
                () -> loader.load("dataset", "code", "schema-v1",
                        "dictionary-v1", "plan-v2"));

        RahaColumnModel restored = releaseManager(repository, 5000L).rollback(
                "dataset", "code", version("rollback-first"));

        assertEquals(first.getModelVersion(), restored.getModelVersion());
        assertEquals(ModelStatus.PUBLISHED, repository.findPublished(
                "dataset", "code").get().getStatus());
        assertEquals(first.getModelVersion(), loader.load("dataset", "code",
                "schema-v1", "dictionary-v1", "plan-v1").getModelVersion());
        RahaColumnModel storedNever = repository.find(
                "dataset", "code", neverPublished.getModelVersion()).get();
        assertEquals(ModelStatus.DISABLED, storedNever.getStatus());
        assertFalse(storedNever.getPublishedAt() != null);
        assertTrue(repository.find("dataset", "code",
                second.getModelVersion()).get().getPublishedAt() > 0L);
    }

    @Test
    void shouldRejectCandidateWhenQualityGateFailed() {
        FileColumnModelStore store = new FileColumnModelStore(temporaryDirectory);
        ColumnModelArtifact artifact = artifact("rejected", 40.0d, 0.0d);
        Map<String, Double> metrics = new LinkedHashMap<String, Double>();
        metrics.put("qualityGatePassed", 0.0d);
        RahaColumnModel model = new RahaColumnModel(artifact.getModelName(),
                artifact.getModelVersion(), "dataset", artifact.getColumnName(),
                "schema-v1", artifact.getClassifierType(),
                artifact.getFeatureDictionaryVersion(), "plan-v1",
                artifact.getThreshold(), store.save(artifact, persistenceContext()),
                ModelStatus.DRAFT,
                metrics, 1000L);
        ModelMetadataRepository repository = new DefaultModelMetadataRepository(
                new InMemoryRahaRepository());

        assertThrows(IllegalStateException.class,
                () -> releaseManager(repository, 2000L).markCandidate(
                        model, version("candidate-rejected")));
        assertFalse(repository.find("dataset", "code",
                artifact.getModelVersion()).isPresent());
    }

    private static ModelReleaseManager releaseManager(ModelMetadataRepository repository,
                                                       long currentTime) {
        return new ModelReleaseManager(repository,
                Clock.fixed(Instant.ofEpochMilli(currentTime), ZoneOffset.UTC));
    }

    private static RahaColumnModel metadata(ColumnModelArtifact artifact,
                                            String modelPath,
                                            long createdAt) {
        return new RahaColumnModel(artifact.getModelName(), artifact.getModelVersion(),
                "dataset", artifact.getColumnName(), "schema-v1",
                artifact.getClassifierType(), artifact.getFeatureDictionaryVersion(),
                "plan-v1", artifact.getThreshold(), modelPath, ModelStatus.DRAFT,
                Collections.singletonMap("sampleCount", 6.0d), createdAt);
    }

    private static ColumnModelArtifact artifact(String suffix,
                                                double intercept,
                                                double coefficient) {
        String version = HashUtils.md5Hex("model-" + suffix);
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        coefficients.put(0, coefficient);
        return new ColumnModelArtifact("raha-code", version, "code",
                ClassifierType.LOGISTIC_REGRESSION, "dictionary-v1", 1,
                0.5d, intercept, coefficients, "test-logistic");
    }

    private static SparseFeatureRow row(double value) {
        return new SparseFeatureRow("cell-1", "code", "dictionary-v1",
                Collections.singletonMap(0, value),
                Collections.<String, String>emptyMap());
    }

    private static ArtifactVersion version(String stageId) {
        return new ArtifactVersion("config-v1", "snapshot-v1", stageId, 1);
    }

    private static ModelPersistenceContext persistenceContext() {
        return new ModelPersistenceContext(HashUtils.md5Hex("model-set-v1"),
                "dataset", "training-batch-v1", ModelStatus.CANDIDATE,
                "plan-v1", "merge-v1",
                Collections.<String, Double>emptyMap(), 1000L, null);
    }
}
