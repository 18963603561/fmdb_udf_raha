package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.config.core.RahaConfigurationException;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbColumnArtifact;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbWriteMode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 FMDB 总开关、九张表开关、列级产物开关和依赖约束。
 */
class FmdbPersistenceConfigTest {

    @Test
    void shouldLoadRecommendedDefaults() {
        FmdbPersistenceConfig config = FmdbPersistenceConfig.fromDefaults();

        assertTrue(config.isEnabled());
        assertTrue(config.isAutoCreateTables());
        assertTrue(config.getSchemaResource().endsWith("raha-fmdb-schema.sql"));
        assertTrue(config.shouldPersist(FmdbPhysicalTable.SAMPLE_RECORD));
        assertTrue(config.shouldPersist(FmdbPhysicalTable.MODEL_ARTIFACT));
        assertFalse(config.shouldPersist(FmdbPhysicalTable.TRAINING_CELL));
        assertTrue(config.shouldPersist(FmdbPhysicalTable.JOB_STAGE_ATTEMPT));
        assertTrue(config.shouldPersist(FmdbColumnArtifact.PROFILE));
        assertTrue(config.shouldPersist(FmdbColumnArtifact.FEATURE_DICTIONARY));
        assertFalse(config.shouldPersist(FmdbColumnArtifact.CLUSTER_SUMMARY));
        assertFalse(config.shouldPersist(FmdbColumnArtifact.PROPAGATION_SUMMARY));
        assertEquals(FmdbWriteMode.DIRECT_APPEND, config.getWriteMode());
        assertTrue(config.isDirectAppend());
    }

    @Test
    void shouldKeepBuilderAndPropertyDefaultsConsistent() {
        FmdbPersistenceConfig properties = FmdbPersistenceConfig.fromDefaults();
        FmdbPersistenceConfig builder = FmdbPersistenceConfig.builder().build();

        for (FmdbPhysicalTable table : FmdbPhysicalTable.values()) {
            assertEquals(properties.isTableEnabled(table),
                    builder.isTableEnabled(table), table.getConfigKey());
        }
        for (FmdbColumnArtifact artifact : FmdbColumnArtifact.values()) {
            assertEquals(properties.isColumnArtifactEnabled(artifact),
                    builder.isColumnArtifactEnabled(artifact),
                    artifact.getConfigKey());
        }
        assertEquals(properties.getWriteMode(), builder.getWriteMode());
    }

    @Test
    void shouldApplyGlobalAndIndependentTableSwitches() {
        FmdbPersistenceConfig disabled = FmdbPersistenceConfig.builder()
                .enabled(false)
                .build();
        FmdbPersistenceConfig detectionDisabled = FmdbPersistenceConfig.builder()
                .table(FmdbPhysicalTable.DETECTION_RESULT, false)
                .build();

        assertFalse(disabled.shouldPersist(FmdbPhysicalTable.SAMPLE_RECORD));
        assertFalse(disabled.shouldPersist(FmdbColumnArtifact.PROFILE));
        assertFalse(detectionDisabled.shouldPersist(
                FmdbPhysicalTable.DETECTION_RESULT));
        assertTrue(detectionDisabled.shouldPersist(FmdbPhysicalTable.JOB_RUN));
    }

    @Test
    void shouldRejectMissingModelDependencies() {
        assertThrows(RahaConfigurationException.class,
                () -> FmdbPersistenceConfig.builder()
                        .table(FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT, false)
                        .build());
        assertThrows(RahaConfigurationException.class,
                () -> FmdbPersistenceConfig.builder()
                        .columnArtifact(FmdbColumnArtifact.FEATURE_DICTIONARY, false)
                        .build());
        assertThrows(RahaConfigurationException.class,
                () -> FmdbPersistenceConfig.builder()
                        .table(FmdbPhysicalTable.JOB_RUN, false)
                        .table(FmdbPhysicalTable.JOB_STAGE_ATTEMPT, true)
                        .build());
    }
}
