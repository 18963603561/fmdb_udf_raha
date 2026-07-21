package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.service.task.ExecutionOverrideOptions;
import com.fiberhome.ml.raha.service.task.FmdbInputSpec;
import com.fiberhome.ml.raha.util.FormDataCodec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 解析三函数 UDF 入参，支持表单编码和简单 JSON 对象两种格式。
 */
public final class RahaUdfRequestParser {

    /** 原始入参文本。 */
    private final String rawText;
    /** 归一化后的字符串参数表。 */
    private final Map<String, String> values;

    private RahaUdfRequestParser(String rawText, Map<String, String> values) {
        this.rawText = rawText;
        this.values = values;
    }

    public static RahaUdfRequestParser parse(String text) {
        if (text == null || text.trim().isEmpty()) {
            throw new RahaUdfException("INVALID_ARGUMENT", "UDF 入参不能为空");
        }
        String trimmed = text.trim();
        Map<String, String> parsed = trimmed.startsWith("{")
                ? parseJson(trimmed) : FormDataCodec.decode(trimmed);
        return new RahaUdfRequestParser(trimmed, parsed);
    }

    public String getRawText() {
        return rawText;
    }

    public Map<String, String> values() {
        return values;
    }

    public String optional(String key) {
        String value = values.get(key);
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    public String optional(String key, String defaultValue) {
        String value = optional(key);
        return value == null ? defaultValue : value;
    }

    public String required(String key, String name) {
        String value = optional(key);
        if (value == null) {
            throw new RahaUdfException("INVALID_ARGUMENT", name + "不能为空");
        }
        return value;
    }

    public boolean bool(String key, boolean defaultValue) {
        String value = optional(key);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value) || "1".equals(value)
                || "yes".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value) || "0".equals(value)
                || "no".equalsIgnoreCase(value)) {
            return false;
        }
        throw new RahaUdfException("INVALID_ARGUMENT",
                key + " 必须为 true 或 false");
    }

    public Integer intOptional(String key) {
        String value = optional(key);
        if (value == null) {
            return null;
        }
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException exception) {
            throw new RahaUdfException("INVALID_ARGUMENT",
                    key + " 必须为整数", exception);
        }
    }

    public int intValue(String key, int defaultValue) {
        Integer value = intOptional(key);
        return value == null ? defaultValue : value.intValue();
    }

    public FmdbInputSpec inputSpec(boolean requireInput) {
        String sourceType = sourceType();
        if (sourceType == null && !requireInput) {
            return null;
        }
        if ("SQL".equals(sourceType)) {
            String sql = firstRequired("sqlText", "sql", "SQL 文本");
            FmdbInputSpec parsed = FmdbInputSpec.sql(sql);
            return withCommonInputOptions(new FmdbInputSpec(
                    parsed.getDatasetId(), parsed.getInputReference(),
                    parsed.getTableName(), DataFormat.FMDB_SQL, rowIdentity(),
                    optional("snapshotId"),
                    optional("sourceVersion"), Collections.<String, String>emptyMap(),
                    csvSet("includeColumns"), csvSet("excludeColumns"),
                    csvSet("sensitiveColumns")));
        }
        if ("TABLE".equals(sourceType)) {
            String tableName = firstRequired("tableName", "table", "FMDB 表名");
            String datasetId = optional("datasetId");
            if (datasetId == null) {
                datasetId = FmdbInputSpec.datasetIdFromTable(tableName);
            }
            return withCommonInputOptions(new FmdbInputSpec(datasetId, tableName,
                    tableName, DataFormat.FMDB_TABLE, rowIdentity(),
                    optional("snapshotId"), optional("sourceVersion"),
                    Collections.<String, String>emptyMap(),
                    csvSet("includeColumns"), csvSet("excludeColumns"),
                    csvSet("sensitiveColumns")));
        }
        throw new RahaUdfException("INVALID_ARGUMENT",
                "sourceType 只支持 TABLE 或 SQL");
    }

    public String sourceType() {
        String sourceType = optional("sourceType");
        if (sourceType != null) {
            return sourceType.trim().toUpperCase(Locale.ROOT);
        }
        if (optional("sqlText") != null || optional("sql") != null) {
            return "SQL";
        }
        if (optional("tableName") != null || optional("table") != null) {
            return "TABLE";
        }
        return null;
    }

    public String inputReferenceSummary(FmdbInputSpec input) {
        if (input == null) {
            return null;
        }
        if (input.getFormat() == DataFormat.FMDB_SQL) {
            return input.getSourceReference();
        }
        return input.getInputReference();
    }

    public String caller() {
        return optional("caller");
    }

    public ExecutionOverrideOptions executionOverrideOptions() {
        return new ExecutionOverrideOptions(bool("forceRun", false),
                optional("forceRunId"));
    }

    private FmdbInputSpec withCommonInputOptions(FmdbInputSpec input) {
        return input;
    }

    private RowIdentityConfig rowIdentity() {
        List<String> columns = csvList("rowKeyColumns");
        if (columns.isEmpty()) {
            return null;
        }
        return RowIdentityConfig.sourceKey(
                columns.toArray(new String[columns.size()]));
    }

    private String firstRequired(String firstKey,
                                 String secondKey,
                                 String name) {
        String value = optional(firstKey);
        if (value == null) {
            value = optional(secondKey);
        }
        if (value == null) {
            throw new RahaUdfException("INVALID_ARGUMENT", name + "不能为空");
        }
        return value;
    }

    private Set<String> csvSet(String key) {
        List<String> values = csvList(key);
        if (values.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<String>(values));
    }

    private List<String> csvList(String key) {
        String text = optional(key);
        if (text == null) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<String>();
        for (String item : text.split(",", -1)) {
            String value = item.trim();
            if (!value.isEmpty()) {
                result.add(value);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static Map<String, String> parseJson(String text) {
        Map<String, Object> object = FmdbJsonCodec.readObject(text);
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Object> entry : object.entrySet()) {
            if (entry.getKey() == null || entry.getKey().trim().isEmpty()) {
                throw new RahaUdfException("INVALID_ARGUMENT",
                        "JSON 参数键不能为空");
            }
            Object value = entry.getValue();
            if (value == null) {
                result.put(entry.getKey(), "");
            } else if (value instanceof List) {
                result.put(entry.getKey(), joinList((List<?>) value));
            } else {
                result.put(entry.getKey(), String.valueOf(value));
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static String joinList(List<?> values) {
        StringBuilder text = new StringBuilder();
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (text.length() > 0) {
                text.append(',');
            }
            text.append(String.valueOf(value));
        }
        return text.toString();
    }
}
