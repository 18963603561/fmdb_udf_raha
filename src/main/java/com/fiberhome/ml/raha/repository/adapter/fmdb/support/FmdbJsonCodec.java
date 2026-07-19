package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 提供稳定键顺序和严格异常转换的结构化 JSON 编解码。
 */
public final class FmdbJsonCodec {

    /** 线程安全的 JSON 映射器。 */
    private static final ObjectMapper MAPPER = createMapper();
    /** 通用对象映射类型。 */
    private static final TypeReference<Map<String, Object>> OBJECT_TYPE =
            new TypeReference<Map<String, Object>>() { };

    private FmdbJsonCodec() {
    }

    /**
     * 将对象编码为稳定 JSON。
     *
     * @param value 待编码对象
     * @return JSON 文本
     */
    public static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("FMDB JSON 编码失败", exception);
        }
    }

    /**
     * 将 JSON 对象解码为字符串到对象映射。
     *
     * @param json JSON 对象文本
     * @return 有序对象映射
     */
    public static Map<String, Object> readObject(String json) {
        if (json == null || json.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            Map<String, Object> result = MAPPER.readValue(json, OBJECT_TYPE);
            return result == null ? Collections.<String, Object>emptyMap()
                    : Collections.unmodifiableMap(
                    new LinkedHashMap<String, Object>(result));
        } catch (IOException exception) {
            throw new IllegalArgumentException("FMDB JSON 解码失败", exception);
        }
    }

    private static ObjectMapper createMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return mapper;
    }
}
