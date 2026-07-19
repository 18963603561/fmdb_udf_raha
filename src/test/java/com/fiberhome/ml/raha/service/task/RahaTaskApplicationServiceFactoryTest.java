package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.core.RahaStorageMode;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证默认应用服务工厂和无参构造器的装配边界。
 */
class RahaTaskApplicationServiceFactoryTest {

    /** 默认模型文件目录。 */
    @TempDir
    Path modelDirectory;

    @AfterEach
    void clearActiveSparkSession() {
        SparkSession.clearActiveSession();
    }

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldReadStorageModeFromDefaultProperties() {
        RahaStorageMode storageMode = RahaDefaultConfigProvider.properties()
                .getEnum("raha.runtime.storage-mode", RahaStorageMode.class);

        assertEquals(RahaStorageMode.IN_MEMORY, storageMode);
    }

    @Test
    void shouldCreateDefaultServiceWithExplicitSparkSession() {
        RahaTaskApplicationService service =
                RahaTaskApplicationServiceFactory.createDefault(
                        SparkTestSession.get(), modelDirectory,
                        RahaStorageMode.IN_MEMORY);

        assertNotNull(service);
    }

    @Test
    void shouldCreateServiceWithNoArgumentConstructorWhenActiveSparkExists() {
        SparkSession sparkSession = SparkTestSession.get();
        SparkSession.setActiveSession(sparkSession);

        RahaTaskApplicationService service = new RahaTaskApplicationService();

        assertNotNull(service);
    }

    @Test
    void shouldFailFastWithoutActiveSparkWhenUsingNoArgumentConstructor() {
        SparkSession.clearActiveSession();

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new RahaTaskApplicationService());

        assertTrue(exception.getMessage().contains("SparkSession"));
    }
}
