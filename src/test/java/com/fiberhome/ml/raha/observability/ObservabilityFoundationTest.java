package com.fiberhome.ml.raha.observability;

import com.fiberhome.ml.raha.error.RahaErrorCategory;
import com.fiberhome.ml.raha.error.RahaErrorCode;
import com.fiberhome.ml.raha.error.RahaException;
import com.fiberhome.ml.raha.util.HashUtils;
import java.util.Collections;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证统一日志上下文、错误分类和敏感值日志检查。
 */
class ObservabilityFoundationTest {

    @Test
    void shouldExposeStableLogContextAndErrorCategory() {
        RahaLogContext context = new RahaLogContext("job", "stage", 2, "snapshot");
        RahaException exception = new RahaException(
                RahaErrorCode.STORAGE_WRITE_FAILED, "写入失败", true);

        assertTrue(context.toLogText().contains("attemptId=2"));
        assertTrue(context.toLogText().contains("snapshotId=snapshot"));
        assertEquals(RahaErrorCategory.STORAGE, exception.getCategory());
        assertTrue(exception.isRecoverable());
    }

    @Test
    void shouldRejectFullSensitiveValueButAcceptHash() {
        String sensitiveValue = "13800000000";

        assertThrows(IllegalArgumentException.class, () ->
                SensitiveLogGuard.requireSafe("phone=" + sensitiveValue,
                        Collections.singletonList(sensitiveValue)));
        SensitiveLogGuard.requireSafe("phoneHash=" + HashUtils.sha256Hex(sensitiveValue),
                Collections.singletonList(sensitiveValue));
    }
}
