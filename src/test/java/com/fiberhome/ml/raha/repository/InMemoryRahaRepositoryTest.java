package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.data.ColumnProfile;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证统一仓储去重、版本更新、分区查询和事务回滚。
 */
class InMemoryRahaRepositoryTest {

    @Test
    void shouldDeduplicateSameKeyAndVersionWhileRefreshingPayload() {
        InMemoryRahaRepository repository = new InMemoryRahaRepository();
        RepositoryKey key = new RepositoryKey(
                RepositoryNamespace.DETECTION_RESULT, "job-1", "cell-1");
        ArtifactVersion firstVersion = new ArtifactVersion(
                "config-v1", "snapshot-v1", "stage-1", 1);

        assertEquals(SaveOutcome.CREATED, repository.save(
                new RepositoryRecord<String>(key, firstVersion, "first", 1L)));
        assertEquals(SaveOutcome.UNCHANGED, repository.save(
                new RepositoryRecord<String>(key, firstVersion, "latest", 2L)));

        assertEquals(1, repository.size());
        assertEquals("latest", repository.find(key, String.class).get().getPayload());
        assertEquals(2L, repository.find(key, String.class).get().getUpdatedAt());
    }

    @Test
    void shouldUpdateWhenArtifactVersionChanges() {
        InMemoryRahaRepository repository = new InMemoryRahaRepository();
        RepositoryKey key = new RepositoryKey(
                RepositoryNamespace.SPARSE_FEATURE, "job-1", "cell-1");
        repository.save(new RepositoryRecord<String>(key,
                new ArtifactVersion("config-v1", "snapshot-v1", "stage-1", 1), "v1", 1L));

        SaveOutcome outcome = repository.save(new RepositoryRecord<String>(key,
                new ArtifactVersion("config-v2", "snapshot-v1", "stage-2", 1), "v2", 2L));

        assertEquals(SaveOutcome.UPDATED, outcome);
        assertEquals(1, repository.size());
        assertEquals("v2", repository.find(key, String.class).get().getPayload());
    }

    @Test
    void shouldRollbackTransactionOnFailure() {
        InMemoryRahaRepository repository = new InMemoryRahaRepository();
        RepositoryRecord<String> initial = record("initial", "record-1", 1L);
        repository.save(initial);

        assertThrows(IllegalStateException.class, () -> repository.executeInTransaction(transactionRepository -> {
            transactionRepository.save(record("temporary", "record-2", 2L));
            throw new IllegalStateException("模拟事务失败");
        }));

        assertEquals(1, repository.size());
        assertTrue(repository.find(initial.getKey(), String.class).isPresent());
        assertFalse(repository.find(record("temporary", "record-2", 2L).getKey(), String.class).isPresent());
    }

    @Test
    void shouldPersistAndQueryColumnProfilesBySnapshot() {
        InMemoryRahaRepository repository = new InMemoryRahaRepository();
        ColumnProfileRepository profileRepository = new DefaultColumnProfileRepository(repository);
        ArtifactVersion version = new ArtifactVersion(
                "config-v1", "snapshot-v1", "profile-stage", 1);
        ColumnProfile first = new ColumnProfile(
                "city", 10L, 0L, 3L, 2, 8, 0.0d, Collections.singletonMap("LETTER", 10L));
        ColumnProfile second = new ColumnProfile(
                "zip", 10L, 0L, 10L, 5, 5, 1.0d, Collections.singletonMap("INTEGER", 10L));

        profileRepository.save("dataset", "snapshot-v1", first, version, 1L);
        profileRepository.save("dataset", "snapshot-v1", second, version, 1L);

        assertEquals(2, profileRepository.findBySnapshot("dataset", "snapshot-v1").size());
        assertEquals("city", profileRepository.find("dataset", "snapshot-v1", "city")
                .get().getColumnName());
    }

    private static RepositoryRecord<String> record(String payload, String recordKey, long updatedAt) {
        return new RepositoryRecord<String>(
                new RepositoryKey(RepositoryNamespace.JOB, "partition", recordKey),
                new ArtifactVersion("config-v1", "snapshot-v1", "stage-1", 1),
                payload, updatedAt);
    }
}

