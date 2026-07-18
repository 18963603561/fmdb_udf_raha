package com.fiberhome.ml.raha;

import com.fiberhome.ml.raha.config.RahaJobConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 验证根包路径没有偏离当前工程约定。
 */
class DependencyBaselineTest {

    @Test
    void shouldUseExpectedRootPackage() {
        assertEquals("com.fiberhome.ml.raha.config", RahaJobConfig.class.getPackage().getName());
    }
}
