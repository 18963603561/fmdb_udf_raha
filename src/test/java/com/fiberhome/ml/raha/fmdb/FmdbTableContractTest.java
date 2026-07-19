package com.fiberhome.ml.raha.fmdb;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbSchemaInitializer;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 逐字段验证九张物理表的 DDL 与 Java 写入模式完全一致。
 */
class FmdbTableContractTest {

    /** 默认建表脚本资源。 */
    private static final String SCHEMA_RESOURCE =
            "db/fmdb/raha-fmdb-schema.sql";
    /** 建表语句中的表名和字段块。 */
    private static final Pattern CREATE_TABLE = Pattern.compile(
            "(?is)^CREATE\\s+TABLE\\s+IF\\s+NOT\\s+EXISTS\\s+"
                    + "([a-zA-Z0-9_.]+)\\s*\\((.*)\\)\\s*USING\\s+ORC");
    /** 可选的物理分区字段块。 */
    private static final Pattern PARTITIONS = Pattern.compile(
            "(?is)PARTITIONED\\s+BY\\s*\\(([^)]*)\\)");

    @Test
    void shouldMatchNineTableFieldsTypesNullabilityAndPartitions()
            throws Exception {
        Map<String, DdlTable> ddlTables = parseTables(readResource());

        assertEquals(FmdbPhysicalTable.values().length, ddlTables.size());
        for (FmdbPhysicalTable table : FmdbPhysicalTable.values()) {
            DdlTable ddl = ddlTables.get(table.getTableName());
            List<DdlField> expected = new ArrayList<DdlField>();
            for (StructField field : FmdbTableSchemas.schema(table).fields()) {
                expected.add(new DdlField(field.name(), sqlType(field),
                        field.nullable()));
            }
            assertEquals(expected, ddl.fields, table.getTableName());
            assertEquals(table.getPartitionColumns(), ddl.partitionColumns,
                    table.getTableName());
        }
    }

    private static Map<String, DdlTable> parseTables(String script) {
        Map<String, DdlTable> tables = new LinkedHashMap<String, DdlTable>();
        for (String statement : FmdbSchemaInitializer.parseStatements(script)) {
            Matcher create = CREATE_TABLE.matcher(statement);
            if (!create.find()) {
                continue;
            }
            String tableName = create.group(1).toLowerCase(Locale.ROOT);
            List<DdlField> fields = new ArrayList<DdlField>();
            for (String declaration : create.group(2).split(",")) {
                String[] tokens = declaration.trim().split("\\s+");
                if (tokens.length < 2) {
                    throw new IllegalArgumentException(
                            "无法解析 FMDB 字段声明：" + declaration);
                }
                boolean nullable = tokens.length < 4
                        || !"NOT".equalsIgnoreCase(tokens[2])
                        || !"NULL".equalsIgnoreCase(tokens[3]);
                fields.add(new DdlField(tokens[0],
                        tokens[1].toUpperCase(Locale.ROOT), nullable));
            }
            Matcher partitions = PARTITIONS.matcher(statement);
            List<String> partitionColumns = new ArrayList<String>();
            if (partitions.find()) {
                for (String column : partitions.group(1).split(",")) {
                    partitionColumns.add(column.trim());
                }
            }
            DdlTable previous = tables.put(tableName,
                    new DdlTable(fields, partitionColumns));
            if (previous != null) {
                throw new IllegalArgumentException("FMDB 表重复定义：" + tableName);
            }
        }
        return tables;
    }

    private static String sqlType(StructField field) {
        if (field.dataType().equals(DataTypes.StringType)) {
            return "STRING";
        }
        if (field.dataType().equals(DataTypes.IntegerType)) {
            return "INT";
        }
        if (field.dataType().equals(DataTypes.LongType)) {
            return "BIGINT";
        }
        if (field.dataType().equals(DataTypes.DoubleType)) {
            return "DOUBLE";
        }
        throw new IllegalArgumentException("未支持的 FMDB 字段类型："
                + field.dataType().catalogString());
    }

    private static String readResource() throws Exception {
        InputStream stream = FmdbTableContractTest.class.getClassLoader()
                .getResourceAsStream(SCHEMA_RESOURCE);
        if (stream == null) {
            throw new IllegalStateException("测试建表脚本不存在：" + SCHEMA_RESOURCE);
        }
        StringBuilder text = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                text.append(line).append('\n');
            }
        }
        return text.toString();
    }

    /** DDL 表契约。 */
    private static final class DdlTable {

        /** 有序字段。 */
        private final List<DdlField> fields;
        /** 有序分区字段。 */
        private final List<String> partitionColumns;

        private DdlTable(List<DdlField> fields,
                         List<String> partitionColumns) {
            this.fields = Collections.unmodifiableList(
                    new ArrayList<DdlField>(fields));
            this.partitionColumns = Collections.unmodifiableList(
                    new ArrayList<String>(partitionColumns));
        }
    }

    /** DDL 字段契约。 */
    private static final class DdlField {

        /** 字段名。 */
        private final String name;
        /** SQL 字段类型。 */
        private final String type;
        /** 是否允许空值。 */
        private final boolean nullable;

        private DdlField(String name, String type, boolean nullable) {
            this.name = name;
            this.type = type;
            this.nullable = nullable;
        }

        @Override
        public boolean equals(Object value) {
            if (this == value) {
                return true;
            }
            if (!(value instanceof DdlField)) {
                return false;
            }
            DdlField other = (DdlField) value;
            return nullable == other.nullable
                    && name.equals(other.name) && type.equals(other.type);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[] {name, type, nullable});
        }

        @Override
        public String toString() {
            return name + " " + type + (nullable ? "" : " NOT NULL");
        }
    }
}
