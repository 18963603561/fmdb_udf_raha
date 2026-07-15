package com.fiberhome.ml.raha.fmdb;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 FMDB 唯一 classpath、缺失依赖、互斥组件和忽略目录规则。
 */
class FmdbClasspathValidatorTest {

    @Test
    void shouldAcceptConfirmedUniqueClasspath() {
        FmdbClasspathManifest manifest = FmdbClasspathManifest.loadDefault();
        List<Path> paths = requiredPaths(manifest, "F:/runtime/lib/");

        List<String> validated = new FmdbClasspathValidator(manifest).validate(paths);

        assertEquals(manifest.getRequiredJars().size(), validated.size());
        assertEquals("3.3.1", manifest.getSparkVersion());
        assertEquals("2.12", manifest.getScalaBinaryVersion());
    }

    @Test
    void shouldRejectMissingAndExcludedJar() {
        FmdbClasspathManifest manifest = FmdbClasspathManifest.loadDefault();
        List<Path> missing = requiredPaths(manifest, "F:/runtime/lib/");
        missing.remove(0);
        List<Path> excluded = requiredPaths(manifest, "F:/runtime/lib/");
        excluded.add(Paths.get("F:/runtime/lib/sql-extended-functions-2.3.0-SNAPSHOT.jar"));

        assertThrows(FmdbClasspathException.class,
                () -> new FmdbClasspathValidator(manifest).validate(missing));
        assertThrows(FmdbClasspathException.class,
                () -> new FmdbClasspathValidator(manifest).validate(excluded));
    }

    @Test
    void shouldIgnoreJarDirectoryAndRejectIncompatibleScala() {
        FmdbClasspathManifest manifest = FmdbClasspathManifest.loadDefault();
        List<Path> ignoredDirectory = requiredPaths(manifest, "F:/runtime/lib/");
        ignoredDirectory.set(0, Paths.get("F:/runtime/lib2.12/"
                + ignoredDirectory.get(0).getFileName()));
        List<Path> incompatible = requiredPaths(manifest, "F:/runtime/lib/");
        incompatible.add(Paths.get("F:/runtime/lib/spark-sql_2.13-4.0.0.jar"));

        assertEquals(manifest.getRequiredJars().size(),
                new FmdbClasspathValidator(manifest).validate(ignoredDirectory).size());
        assertThrows(FmdbClasspathException.class,
                () -> new FmdbClasspathValidator(manifest).validate(incompatible));
    }

    @Test
    void shouldRejectMultipleVersionsOfSameComponent() {
        FmdbClasspathManifest manifest = FmdbClasspathManifest.loadDefault();
        List<Path> paths = requiredPaths(manifest, "F:/runtime/lib/");
        paths.add(Paths.get("F:/runtime/lib/sql-common-2.2.0-SNAPSHOT.jar"));

        FmdbClasspathException exception = assertThrows(FmdbClasspathException.class,
                () -> new FmdbClasspathValidator(manifest).validate(paths));

        assertTrue(exception.getMessage().contains("多个版本"));
    }

    private static List<Path> requiredPaths(FmdbClasspathManifest manifest,
                                            String directory) {
        List<Path> paths = new ArrayList<Path>();
        for (String fileName : manifest.getRequiredJars()) {
            paths.add(Paths.get(directory + fileName));
        }
        return paths;
    }
}
