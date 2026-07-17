package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 UDF 固定注册名称和请求长度边界。
 */
class RahaUdfConfigurationTest {

    @Test
    void shouldApplyFixedRequestLengthAndFunctionNames() {
        RahaUdfException exception = assertThrows(RahaUdfException.class,
                () -> new RahaUdfRequestParser(10)
                        .parse(RahaTaskType.DETECT, "12345678901"));

        assertEquals("INVALID_UDF_ARGUMENT", exception.getErrorCode());
        assertEquals("F_DW_RAHATRAIN", RahaUdfRegistrar.TRAIN_FUNCTION);
        assertEquals("F_DW_RAHADETECT", RahaUdfRegistrar.DETECT_FUNCTION);
        assertEquals("F_DW_RAHASAMPLE", RahaUdfRegistrar.SAMPLE_FUNCTION);
    }
}
