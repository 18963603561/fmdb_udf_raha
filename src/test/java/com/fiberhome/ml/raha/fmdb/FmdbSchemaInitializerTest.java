package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbSchemaInitializer;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 FMDB 默认建表脚本解析和初始化开关。
 */
class FmdbSchemaInitializerTest {

    /** 默认建表脚本资源。 */
    private static final String SCHEMA_RESOURCE = "db/fmdb/raha-fmdb-schema.sql";

    @Test
    void shouldParseCommentsAndQuotedSemicolons() {
        List<String> statements = FmdbSchemaInitializer.parseStatements(
                "-- 注释\nCREATE DATABASE IF NOT EXISTS test_db;\n"
                        + "CREATE TABLE test_db.t (v STRING) USING ORC "
                        + "TBLPROPERTIES ('note'='a;b');");

        assertEquals(2, statements.size());
        assertTrue(statements.get(1).contains("a;b"));
    }

    @Test
    void shouldParseNineTableSchemaWithSparkSql() throws Exception {
        SparkSession spark = SparkTestSession.get();
        String script = readResource(SCHEMA_RESOURCE);
        List<String> statements = FmdbSchemaInitializer.parseStatements(script);

        assertEquals(10, statements.size());
        for (String statement : statements) {
            // 只调用 Spark SQL 解析器，避免 Windows 本地测试依赖 winutils.exe。
            spark.sessionState().sqlParser().parsePlan(statement);
        }
        assertEquals(9, count(script, "CREATE TABLE IF NOT EXISTS"));
        assertTrue(script.contains("raha_training_cell"));
        assertTrue(script.contains("raha_training_example"));
        assertTrue(script.contains("cell_value STRING"));
        assertFalse(script.contains("cell_value_hash"));
    }

    @Test
    void shouldSkipInitializationWhenPersistenceIsDisabled() {
        SparkSession spark = SparkTestSession.get();
        FmdbPersistenceConfig config = FmdbPersistenceConfig.builder()
                .enabled(false)
                .build();

        assertDoesNotThrow(() -> new FmdbSchemaInitializer(spark, config).initialize());
    }

    private static String readResource(String resource) throws Exception {
        InputStream stream = FmdbSchemaInitializerTest.class.getClassLoader()
                .getResourceAsStream(resource);
        if (stream == null) {
            throw new IllegalStateException("测试建表脚本不存在：" + resource);
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

    private static int count(String text, String token) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(token, index)) >= 0) {
            count++;
            index += token.length();
        }
        return count;
    }
}
