package com.fiberhome.ml.raha;

import com.fiberhome.ml.raha.config.RahaJobConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * 验证包路径和依赖白名单没有偏离当前工程基线。
 */
class DependencyBaselineTest {

    @Test
    void shouldUseExpectedRootPackageAndDependencyBaseline() throws IOException {
        Properties properties = new Properties();
        InputStream inputStream = DependencyBaselineTest.class.getClassLoader()
                .getResourceAsStream("raha-dependency-whitelist.properties");
        assertNotNull(inputStream);
        try {
            properties.load(inputStream);
        } finally {
            inputStream.close();
        }

        assertEquals("com.fiberhome.ml.raha.config", RahaJobConfig.class.getPackage().getName());
        assertEquals("1.8", properties.getProperty("java.version"));
        assertEquals("3.3.1", properties.getProperty("spark.version"));
        assertEquals("2.12", properties.getProperty("scala.binary.version"));
        assertEquals("confirmed", properties.getProperty("mllib.status"));
        assertEquals("spark-mllib_2.12", properties.getProperty("mllib.artifact"));
        assertEquals("confirmed", properties.getProperty("fmdb.status"));
        assertEquals("provided", properties.getProperty("fmdb.dependency.scope"));
    }
}
