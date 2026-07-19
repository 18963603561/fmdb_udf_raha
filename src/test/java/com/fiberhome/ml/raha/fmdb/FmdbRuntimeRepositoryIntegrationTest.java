package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.domain.ModelPersistenceContext;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.repository.adapter.fmdb.FmdbModelStore;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbJobRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.repository.FmdbModelMetadataRepository;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.SparkSqlFmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证模型元数据和任务状态能从新的 FMDB 仓储实例恢复。 */
class FmdbRuntimeRepositoryIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldRestoreCandidateAndPublishedModelAcrossRepositoryInstances() {
        InMemoryFmdbTableGateway gateway = new InMemoryFmdbTableGateway(
                SparkTestSession.get());
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder().build();
        FmdbModelStore store = new FmdbModelStore(SparkTestSession.get(), gateway,
                FmdbPhysicalTable.MODEL_ARTIFACT.getTableName(),
                FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT.getTableName(),
                fixedClock(2000L), config);
        ColumnModelArtifact artifact = new ColumnModelArtifact("value-model",
                "model-v1", "value", ClassifierType.LOGISTIC_REGRESSION,
                "dict-v1", 1, 0.5d, 0.1d,
                Collections.singletonMap(0, 1.0d), "SPARK_MLLIB");
        String path = store.save(artifact, new ModelPersistenceContext(
                "model-set-v1", "dataset-1", "schema-v1", "training-1",
                ModelStatus.CANDIDATE, "strategy-v1", "merge-v1",
                Collections.singletonMap("accuracy", 1.0d), 2000L, null));
        RahaColumnModel candidate = new RahaColumnModel("value-model", "model-v1",
                "dataset-1", "value", "schema-v1",
                ClassifierType.LOGISTIC_REGRESSION, "dict-v1", "strategy-v1",
                0.5d, path, ModelStatus.CANDIDATE,
                Collections.singletonMap("accuracy", 1.0d), 2000L);
        ArtifactVersion version = new ArtifactVersion("config-v1", "snapshot-v1",
                "stage-v1", 1);
        FmdbModelMetadataRepository first = new FmdbModelMetadataRepository(
                SparkTestSession.get(), gateway, config);
        first.saveAll(Collections.singletonList(candidate), version, 2100L);

        FmdbModelMetadataRepository restarted = new FmdbModelMetadataRepository(
                SparkTestSession.get(), gateway, config);
        RahaColumnModel restored = restarted.find("dataset-1", "value", "model-v1")
                .orElseThrow(() -> new AssertionError("候选模型未恢复"));
        assertEquals(ModelStatus.CANDIDATE, restored.getStatus());
        RahaColumnModel published = restored.withStatus(ModelStatus.PUBLISHED, 3000L);
        restarted.saveAll(Collections.singletonList(published), version, 3000L);

        FmdbModelMetadataRepository secondRestart = new FmdbModelMetadataRepository(
                SparkTestSession.get(), gateway, config);
        assertEquals(ModelStatus.PUBLISHED, secondRestart.findPublished(
                "dataset-1", "value").orElseThrow(
                () -> new AssertionError("已发布模型未恢复")).getStatus());
        assertEquals("model-v1", store.load(path).getModelVersion());
    }

    @Test
    void shouldAppendMonotonicJobStatesAndRestoreLatestState() {
        InMemoryFmdbTableGateway gateway = new InMemoryFmdbTableGateway(
                SparkTestSession.get());
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder().build();
        SparkSqlFmdbResultWriter writer = new SparkSqlFmdbResultWriter(
                SparkTestSession.get(), gateway, fixedClock(5000L), config);
        FmdbJobRepository repository = new FmdbJobRepository(writer, gateway, config);
        RahaJob job = new RahaJob("job-1", "idem-1", JobType.TRAINING,
                "dataset-1", "snapshot-1", "config-v1", 1000L);
        assertEquals(SaveOutcome.CREATED, repository.save(job, 1000L));
        job.start("stage-1", 2000L);
        assertEquals(SaveOutcome.UPDATED, repository.save(job, 2000L));
        assertEquals(SaveOutcome.UNCHANGED, repository.save(job, 2001L));
        job.succeed(3000L);
        assertEquals(SaveOutcome.UPDATED, repository.save(job, 3000L));

        FmdbJobRepository restarted = new FmdbJobRepository(writer, gateway, config);
        RahaJob restored = restarted.findByIdempotentKey("dataset-1", "idem-1")
                .orElseThrow(() -> new AssertionError("任务状态未恢复"));
        assertEquals(JobStatus.SUCCEEDED, restored.getStatus());
        assertEquals("stage-1", restored.getCurrentStageId());
        assertEquals(3L, gateway.read(FmdbPhysicalTable.JOB_RUN.getTableName()).count());
        assertTrue(restored.getFinishedAt() > 0L);
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
