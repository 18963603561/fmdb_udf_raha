package com.fiberhome.ml.raha.support;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 面向 Raha 固定契约的轻量 JSON 工具，避免引入与 Spark 冲突的 JSON 依赖。
 */
public final class JsonUtils {

    private JsonUtils() {
    }

    /**
     * 将简单对象转换为 JSON。支持字符串、数字、布尔值、集合、映射和数组。
     *
     * @param value 简单对象
     * @return JSON 文本
     */
    public static String toJson(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String || value instanceof Character) {
            return quote(String.valueOf(value));
        }
        if (value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        if (value instanceof Map) {
            StringBuilder builder = new StringBuilder("{");
            boolean first = true;
            for (Object entryObject : ((Map<?, ?>) value).entrySet()) {
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) entryObject;
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(quote(String.valueOf(entry.getKey())))
                        .append(':').append(toJson(entry.getValue()));
            }
            return builder.append('}').toString();
        }
        if (value instanceof Collection) {
            StringBuilder builder = new StringBuilder("[");
            boolean first = true;
            for (Object item : (Collection<?>) value) {
                if (!first) {
                    builder.append(',');
                }
                first = false;
                builder.append(toJson(item));
            }
            return builder.append(']').toString();
        }
        if (value instanceof double[]) {
            List<Double> values = new ArrayList<Double>();
            for (double item : (double[]) value) {
                values.add(item);
            }
            return toJson(values);
        }
        return quote(String.valueOf(value));
    }

    /**
     * 解析字符串数组，只接受本工程生成的标准 JSON 数组。
     *
     * @param json JSON 数组
     * @return 字符串列表
     */
    public static List<String> parseStringArray(String json) {
        List<String> values = new ArrayList<String>();
        if (json == null || json.trim().isEmpty() || "null".equals(json.trim())) {
            return values;
        }
        Parser parser = new Parser(json);
        parser.expect('[');
        parser.skipWhitespace();
        while (!parser.peek(']')) {
            values.add(parser.readString());
            parser.skipWhitespace();
            if (parser.peek(',')) {
                parser.expect(',');
            } else {
                break;
            }
        }
        parser.expect(']');
        return values;
    }

    /**
     * 解析 Spark 行 JSON 中的顶层简单字段。
     *
     * @param json 行 JSON
     * @return 保持字段顺序的字符串映射
     */
    public static Map<String, String> parseStringMap(String json) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        if (json == null || json.trim().isEmpty()) {
            return values;
        }
        Parser parser = new Parser(json);
        parser.expect('{');
        parser.skipWhitespace();
        while (!parser.peek('}')) {
            String key = parser.readString();
            parser.expect(':');
            parser.skipWhitespace();
            String value;
            if (parser.peek('"')) {
                value = parser.readString();
            } else {
                value = parser.readLiteral();
                if ("null".equals(value)) {
                    value = null;
                }
            }
            values.put(key, value);
            parser.skipWhitespace();
            if (parser.peek(',')) {
                parser.expect(',');
            } else {
                break;
            }
        }
        parser.expect('}');
        return values;
    }

    /**
     * 从顶层 JSON 对象读取字符串字段。
     *
     * @param json JSON 对象
     * @param field 字段名
     * @return 字段值
     */
    public static String getString(String json, String field) {
        if (json == null || field == null) {
            return null;
        }
        String marker = quote(field) + ':';
        int position = json.indexOf(marker);
        if (position < 0) {
            return null;
        }
        Parser parser = new Parser(json.substring(position + marker.length()));
        parser.skipWhitespace();
        return parser.peek('"') ? parser.readString() : parser.readLiteralValue();
    }

    private static String quote(String value) {
        StringBuilder builder = new StringBuilder("\"");
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '\\':
                    builder.append("\\\\");
                    break;
                case '"':
                    builder.append("\\\"");
                    break;
                case '\n':
                    builder.append("\\n");
                    break;
                case '\r':
                    builder.append("\\r");
                    break;
                case '\t':
                    builder.append("\\t");
                    break;
                default:
                    builder.append(character);
                    break;
            }
        }
        return builder.append('"').toString();
    }

    /**
     * 固定契约 JSON 解析器，只处理字符串数组和顶层简单对象。
     */
    private static final class Parser {
        /** 原始 JSON。 */
        private final String source;
        /** 当前读取位置。 */
        private int index;

        private Parser(String source) {
            this.source = source.trim();
        }

        private void skipWhitespace() {
            while (index < source.length() && Character.isWhitespace(source.charAt(index))) {
                index++;
            }
        }

        private boolean peek(char expected) {
            skipWhitespace();
            return index < source.length() && source.charAt(index) == expected;
        }

        private void expect(char expected) {
            skipWhitespace();
            if (index >= source.length() || source.charAt(index) != expected) {
                throw new RahaException(RahaErrorCode.INVALID_DATA,
                        "JSON 格式不符合预期，位置=" + index + "，期望字符=" + expected);
            }
            index++;
        }

        private String readString() {
            expect('"');
            StringBuilder builder = new StringBuilder();
            while (index < source.length()) {
                char character = source.charAt(index++);
                if (character == '"') {
                    return builder.toString();
                }
                if (character == '\\') {
                    if (index >= source.length()) {
                        break;
                    }
                    char escaped = source.charAt(index++);
                    switch (escaped) {
                        case 'n':
                            builder.append('\n');
                            break;
                        case 'r':
                            builder.append('\r');
                            break;
                        case 't':
                            builder.append('\t');
                            break;
                        default:
                            builder.append(escaped);
                            break;
                    }
                } else {
                    builder.append(character);
                }
            }
            throw new RahaException(RahaErrorCode.INVALID_DATA, "JSON 字符串未闭合");
        }

        private String readLiteral() {
            skipWhitespace();
            int start = index;
            while (index < source.length()
                    && source.charAt(index) != ',' && source.charAt(index) != '}') {
                index++;
            }
            return source.substring(start, index).trim();
        }

        private String readLiteralValue() {
            skipWhitespace();
            int start = index;
            while (index < source.length()
                    && source.charAt(index) != ',' && source.charAt(index) != '}') {
                index++;
            }
            String value = source.substring(start, index).trim();
            return "null".equals(value) ? null : value;
        }
    }
}
