package com.fiberhome.ml.raha.fmdb;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPartitionUtils;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 FMDB JSON 和分区字段协议在跨进程场景下保持稳定。
 */
class FmdbProtocolUtilityTest {

    @Test
    void shouldKeepStableJsonOrderAndValueTypes() {
        Map<String, Object> nested = new LinkedHashMap<String, Object>();
        nested.put("text", "001");
        nested.put("number", 1);
        nested.put("empty", "");
        nested.put("nullable", null);
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("z", nested);
        value.put("a", true);

        String json = FmdbJsonCodec.write(value);
        Map<String, Object> decoded = FmdbJsonCodec.readObject(json);
        Map<?, ?> decodedNested = (Map<?, ?>) decoded.get("z");

        assertEquals("{\"a\":true,\"z\":{\"empty\":\"\","
                + "\"nullable\":null,\"number\":1,\"text\":\"001\"}}", json);
        assertEquals("001", decodedNested.get("text"));
        assertEquals(1, decodedNested.get("number"));
        assertEquals("", decodedNested.get("empty"));
        assertNull(decodedNested.get("nullable"));
        assertEquals("null", FmdbJsonCodec.write(null));
    }

    @Test
    void shouldUseUtcAtMonthAndDayBoundaries() {
        long beforeBoundary = Instant.parse("2026-07-31T23:59:59.999Z")
                .toEpochMilli();
        long afterBoundary = Instant.parse("2026-08-01T00:00:00Z")
                .toEpochMilli();

        assertEquals("2026-07", FmdbPartitionUtils.month(beforeBoundary));
        assertEquals("2026-07-31", FmdbPartitionUtils.date(beforeBoundary));
        assertEquals("2026-08", FmdbPartitionUtils.month(afterBoundary));
        assertEquals("2026-08-01", FmdbPartitionUtils.date(afterBoundary));
        assertThrows(IllegalArgumentException.class,
                () -> FmdbPartitionUtils.month(0L));
    }
}
