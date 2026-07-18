package com.fiberhome.ml.raha.data;

import com.fiberhome.ml.raha.support.RahaException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 目标字段解析规则测试。
 */
class TargetColumnResolverTest {

    @Test
    void shouldUseAllColumnsByDefaultAndPreserveSchemaOrder() {
        assertEquals(Arrays.asList("ID", "Lord", "Kingdom"),
                TargetColumnResolver.resolve(Collections.<String>emptyList(),
                        Arrays.asList("ID", "Lord", "Kingdom")));
        assertEquals(Arrays.asList("ID", "Kingdom"),
                TargetColumnResolver.resolve(Arrays.asList("Kingdom", "ID"),
                        Arrays.asList("ID", "Lord", "Kingdom")));
    }

    @Test
    void shouldRejectDuplicateAndUnknownColumns() {
        assertThrows(RahaException.class, () -> TargetColumnResolver.resolve(
                Arrays.asList("ID", "ID"), Arrays.asList("ID", "Lord")));
        assertThrows(RahaException.class, () -> TargetColumnResolver.resolve(
                Collections.singletonList("missing"), Arrays.asList("ID", "Lord")));
    }
}
