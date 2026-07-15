package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.UdfConfig;
import com.fiberhome.ml.raha.service.RahaTaskType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 UDF 注册名称和请求长度边界由配置对象控制。
 */
class RahaUdfConfigurationTest {

    @Test
    void shouldApplyConfiguredRequestLengthAndFunctionNames() {
        RahaUdfException exception = assertThrows(RahaUdfException.class,
                () -> new RahaUdfRequestParser(10)
                        .parse(RahaTaskType.DETECT, "12345678901"));
        UdfConfig defaults = RahaDefaultConfigProvider.factory().udfConfig();

        assertEquals("INVALID_UDF_ARGUMENT", exception.getErrorCode());
        assertEquals(defaults.getTrainFunction(), RahaUdfRegistrar.TRAIN_FUNCTION);
        assertEquals(defaults.getDetectFunction(), RahaUdfRegistrar.DETECT_FUNCTION);
        assertEquals(defaults.getSampleFunction(), RahaUdfRegistrar.SAMPLE_FUNCTION);
        assertThrows(IllegalArgumentException.class,
                () -> new UdfConfig("F_DUP", "F_DUP", "F_SAMPLE", 100));
    }
}
