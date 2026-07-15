package com.fiberhome.ml.raha.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证值哈希和脱敏边界。
 */
class ValueProtectionUtilsTest {

    @Test
    void shouldHashNullAndRegularValuesDeterministically() {
        assertEquals(ValueProtectionUtils.hashValue(null), ValueProtectionUtils.hashValue(null));
        assertEquals(ValueProtectionUtils.hashValue("abc"), ValueProtectionUtils.hashValue("abc"));
        assertNotEquals(ValueProtectionUtils.hashValue("abc"), ValueProtectionUtils.hashValue("abd"));
    }

    @Test
    void shouldMaskValueWithoutLeakingShortValue() {
        assertEquals("a****f", ValueProtectionUtils.mask("abcdef", 1, 1));
        assertEquals("***", ValueProtectionUtils.mask("abc", 2, 1));
        assertEquals("", ValueProtectionUtils.mask("", 0, 0));
        assertNull(ValueProtectionUtils.mask(null, 1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ValueProtectionUtils.mask("abc", -1, 1));
    }
}

