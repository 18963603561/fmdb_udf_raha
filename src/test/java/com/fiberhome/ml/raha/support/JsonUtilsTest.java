package com.fiberhome.ml.raha.support;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 轻量 JSON 工具测试。
 */
class JsonUtilsTest {

    @Test
    void shouldRoundTripStringCollectionsAndMaps() {
        assertEquals(Arrays.asList("a", "中\"文", ""),
                JsonUtils.parseStringArray(JsonUtils.toJson(
                        Arrays.asList("a", "中\"文", ""))));
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("ID", "1");
        values.put("name", "A,B");
        values.put("empty", null);
        Map<String, String> parsed = JsonUtils.parseStringMap(JsonUtils.toJson(values));
        assertEquals("1", parsed.get("ID"));
        assertEquals("A,B", parsed.get("name"));
        assertEquals(null, parsed.get("empty"));
    }

    @Test
    void shouldReadTopLevelResultFieldWithoutParsingNestedArray() {
        String json = "{\"sampleBatchId\":\"sample_1\","
                + "\"targetColumns\":[\"a\",\"b\"]}";
        assertEquals("sample_1", JsonUtils.getString(json, "sampleBatchId"));
    }
}
