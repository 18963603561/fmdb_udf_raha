package com.fiberhome.ml.raha;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 验证配置对象已经归入明确的数据传输对象包。
 */
class DependencyBaselineTest {

    @Test
    void shouldUseExpectedRootPackage() {
        assertEquals("com.fiberhome.ml.raha.config.dto", RahaJobConfig.class.getPackage().getName());
    }
}
