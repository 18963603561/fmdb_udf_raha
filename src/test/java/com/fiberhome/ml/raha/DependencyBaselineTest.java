package com.fiberhome.ml.raha;

import com.fiberhome.ml.raha.config.RahaJobConfig;
import com.fiberhome.ml.raha.fmdb.FmdbClasspathManifest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证包路径和依赖白名单没有偏离当前工程基线。
 */
class DependencyBaselineTest {

    @Test
    void shouldUseExpectedRootPackageAndDependencyBaseline() {
        FmdbClasspathManifest manifest = FmdbClasspathManifest.loadDefault();

        assertEquals("com.fiberhome.ml.raha.config", RahaJobConfig.class.getPackage().getName());
        assertEquals("3.3.1", manifest.getSparkVersion());
        assertEquals("2.12", manifest.getScalaBinaryVersion());
        assertTrue(manifest.getRequiredJars().contains(
                "spark-mllib_2.12-3.3.1.jar"));
        assertTrue(manifest.getExcludedJars().contains(
                "sql-extended-functions-2.3.0-SNAPSHOT.jar"));
    }
}
