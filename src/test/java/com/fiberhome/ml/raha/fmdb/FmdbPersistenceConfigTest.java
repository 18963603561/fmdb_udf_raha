package com.fiberhome.ml.raha.fmdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 FMDB 持久化总开关和中间产物判断。
 */
class FmdbPersistenceConfigTest {

    @Test
    void shouldLoadEnabledAutoCreateDefaults() {
        FmdbPersistenceConfig config = FmdbPersistenceConfig.fromDefaults();

        assertTrue(config.isEnabled());
        assertTrue(config.isAutoCreateTables());
        assertTrue(config.getSchemaResource().endsWith("raha-fmdb-schema.sql"));
    }

    @Test
    void shouldRequireBothGlobalAndIntermediateSwitches() {
        FmdbPersistenceConfig enabled = new FmdbPersistenceConfig(
                true, true, "schema.sql");
        FmdbPersistenceConfig disabled = new FmdbPersistenceConfig(
                false, true, "schema.sql");

        assertTrue(enabled.shouldPersist(false, false));
        assertFalse(enabled.shouldPersist(true, false));
        assertTrue(enabled.shouldPersist(true, true));
        assertFalse(disabled.shouldPersist(false, true));
    }
}
