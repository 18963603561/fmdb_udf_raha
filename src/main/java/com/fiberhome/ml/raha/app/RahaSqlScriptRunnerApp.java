package com.fiberhome.ml.raha.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地执行单个 Spark SQL 脚本的入口。
 *
 * <p>该工具用于在缺少 spark-sql 命令行时，直接通过 SparkSession 执行建表、插数和
 * 函数注册脚本，便于在本地复现 UDF 运行环境。</p>
 */
public final class RahaSqlScriptRunnerApp {

    /** 日志记录器。 */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RahaSqlScriptRunnerApp.class);

    /** SQL 脚本路径参数下标。 */
    private static final int SCRIPT_ARG_INDEX = 0;
    /** 可选输出目录参数下标。 */
    private static final int OUTPUT_ARG_INDEX = 1;

    private RahaSqlScriptRunnerApp() {
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException(
                    "用法：RahaSqlScriptRunnerApp <sqlFile> [outputDir]");
        }
        Path scriptPath = Paths.get(args[SCRIPT_ARG_INDEX]);
        if (!Files.isRegularFile(scriptPath)) {
            throw new IllegalArgumentException("SQL 文件不存在：" + scriptPath);
        }
        Path outputDir = args.length > OUTPUT_ARG_INDEX
                ? Paths.get(args[OUTPUT_ARG_INDEX]) : null;
        SparkSession spark = SparkSession.builder()
                .appName("RahaSqlScriptRunnerApp")
                .master(System.getProperty("raha.spark.master", "local[*]"))
                .enableHiveSupport()
                .getOrCreate();
        SparkSession.setActiveSession(spark);
        SparkSession.setDefaultSession(spark);
        try {
            spark.sparkContext().setLogLevel("WARN");
            List<String> statements = splitStatements(readSql(scriptPath));
            LOGGER.info("开始执行 SQL 脚本，scriptPath={}，statementCount={}",
                    scriptPath, Integer.valueOf(statements.size()));
            int executed = 0;
            for (String statement : statements) {
                String trimmed = statement.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (trimmed.toUpperCase().startsWith("ADD JAR")) {
                    LOGGER.info("跳过本地不需要的语句：{}", oneLine(trimmed));
                    continue;
                }
                executed++;
                LOGGER.info("执行 SQL 语句 {}/{}：{}", Integer.valueOf(executed),
                        Integer.valueOf(statements.size()), oneLine(trimmed));
                spark.sql(trimmed);
            }
            LOGGER.info("SQL 脚本执行完成，scriptPath={}，executed={}",
                    scriptPath, Integer.valueOf(executed));
            if (outputDir != null) {
                Files.createDirectories(outputDir);
                Path marker = outputDir.resolve("sql-run-summary.txt");
                Files.write(marker,
                        ("scriptPath=" + scriptPath + System.lineSeparator()
                                + "executed=" + executed + System.lineSeparator())
                                .getBytes(StandardCharsets.UTF_8));
                System.out.println("RAHA_SQL_RUN_SUMMARY=" + marker.toAbsolutePath());
            }
        } finally {
            spark.stop();
        }
    }

    /**
     * 读取 SQL 文件内容。
     *
     * @param scriptPath SQL 文件路径
     * @return SQL 文本
     * @throws IOException 读取失败时抛出
     */
    private static String readSql(Path scriptPath) throws IOException {
        byte[] bytes = Files.readAllBytes(scriptPath);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 按分号切分 SQL 语句，避免把字符串字面量内部的分号误切开。
     *
     * @param sql SQL 原文
     * @return 语句列表
     */
    private static List<String> splitStatements(String sql) {
        List<String> statements = new ArrayList<String>();
        if (sql == null || sql.isEmpty()) {
            return statements;
        }
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        boolean inLineComment = false;
        for (int index = 0; index < sql.length(); index++) {
            char ch = sql.charAt(index);
            char next = index + 1 < sql.length() ? sql.charAt(index + 1) : '\0';
            if (inLineComment) {
                if (ch == '\n') {
                    inLineComment = false;
                    current.append(ch);
                }
                continue;
            }
            if (!inQuote && ch == '-' && next == '-') {
                inLineComment = true;
                index++;
                continue;
            }
            if (ch == '\'') {
                current.append(ch);
                if (inQuote && next == '\'') {
                    current.append(next);
                    index++;
                } else {
                    inQuote = !inQuote;
                }
                continue;
            }
            if (!inQuote && ch == ';') {
                statements.add(current.toString());
                current.setLength(0);
                continue;
            }
            current.append(ch);
        }
        if (current.length() > 0) {
            statements.add(current.toString());
        }
        return statements;
    }

    /**
     * 将多行 SQL 压缩为单行日志，方便查看执行进度。
     *
     * @param statement SQL 语句
     * @return 单行文本
     */
    private static String oneLine(String statement) {
        return statement == null ? "" : statement.replace('\r', ' ')
                .replace('\n', ' ').trim();
    }
}
