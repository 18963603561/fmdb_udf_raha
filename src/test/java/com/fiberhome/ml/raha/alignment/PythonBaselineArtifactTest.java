package com.fiberhome.ml.raha.alignment;

import com.fiberhome.ml.raha.util.HashUtils;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证固定数据和实际 Python demo 生成的策略、特征、采样与检测基线未漂移。
 */
class PythonBaselineArtifactTest {

    @Test
    void shouldKeepDeterministicPythonDemoBaseline() throws Exception {
        Properties baseline = properties("alignment/iteration5-python-baseline.properties");
        String dirty = resourceText("alignment/iteration5-dirty.csv");
        String clean = resourceText("alignment/iteration5-clean.csv");

        assertEquals(baseline.getProperty("dirty.sha256"), HashUtils.sha256Hex(dirty));
        assertEquals(baseline.getProperty("clean.sha256"), HashUtils.sha256Hex(clean));
        assertEquals("102", baseline.getProperty("strategy.profile.count"));
        assertEquals("3:code,3:city,4:code,4:city",
                baseline.getProperty("rvd.code.city.cells"));
        assertEquals("7:event_date",
                baseline.getProperty("pvd.event_date.slash.cells"));
        assertEquals("5:city,9:amount,10:event_date",
                baseline.getProperty("od.gaussian.3.cells"));
        assertEquals("48", baseline.getProperty("od.histogram.0.1.0.1.count"));
        assertEquals("6:amount,7:event_date,8:email",
                baseline.getProperty("detected.cells"));
        assertEquals("4:code,5:city,6:amount,7:event_date,8:email,9:amount,10:event_date",
                baseline.getProperty("actual.error.cells"));
        assertTrue(baseline.getProperty("feature.counts").contains("id:13"));
        assertFalse("UNKNOWN".equals(baseline.getProperty("python.demo.revision")));
    }

    private static Properties properties(String resource) throws Exception {
        Properties properties = new Properties();
        try (InputStream stream = PythonBaselineArtifactTest.class
                .getClassLoader().getResourceAsStream(resource)) {
            properties.load(new java.io.InputStreamReader(stream, StandardCharsets.UTF_8));
        }
        return properties;
    }

    private static String resourceText(String resource) throws Exception {
        try (InputStream stream = PythonBaselineArtifactTest.class
                .getClassLoader().getResourceAsStream(resource)) {
            byte[] bytes = new byte[8192];
            java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
            int count;
            while ((count = stream.read(bytes)) >= 0) {
                output.write(bytes, 0, count);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }
}
