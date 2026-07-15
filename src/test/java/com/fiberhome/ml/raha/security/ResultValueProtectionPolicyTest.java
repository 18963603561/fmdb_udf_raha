package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.util.HashUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 验证敏感检测值仅哈希、二次脱敏和非敏感字段保留策略。
 */
class ResultValueProtectionPolicyTest {

    @Test
    void shouldSupportHashOnlyAndConfiguredMaskingModes() {
        DetectionResult sensitive = result("secret", "abcdef");
        ResultValueProtectionPolicy maskedPolicy =
                new ResultValueProtectionPolicy(false,
                        Collections.singleton("secret"), ResultValueMode.MASKED,
                        1, 1);
        ResultValueProtectionPolicy hashOnly =
                ResultValueProtectionPolicy.hashOnlyForAllColumns();

        assertEquals("a****f", maskedPolicy.protectedMaskedValue(sensitive));
        assertNull(hashOnly.protectedMaskedValue(sensitive));
        assertEquals("abcdef", maskedPolicy.protectedMaskedValue(
                result("public_code", "abcdef")));
    }

    private static DetectionResult result(String columnName, String maskedValue) {
        return new DetectionResult("job-1", "config-v1", "persist",
                new CellCoordinate("dataset", "snapshot-v1", "row-1", columnName),
                HashUtils.sha256Hex(maskedValue), maskedValue, true, 0.9d, 0.5d,
                Collections.singletonList("strategy-v1"),
                Collections.singletonMap("reason", "test"), "raha-model",
                "model-v1", "dictionary-v1", 1000L);
    }
}
