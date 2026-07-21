package com.fiberhome.ml.raha.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地执行 Spark SQL 脚本的调试入口。
 *
 * <p>该工具用于在缺少 spark-sql 命令时，直接通过 SparkSession 执行建库、建表和插数脚本。
 * 无参数运行时默认加载 {@code datasets/person_info/sql} 下的人员信息建表脚本，输出摘要写入
 * {@code datasets/person_info/out}。</p>
 */
public final class RahaSqlScriptRunnerApp {

    /** 日志记录器。 */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RahaSqlScriptRunnerApp.class);

    /** SQL 脚本路径参数下标。 */
    private static final int SCRIPT_ARG_INDEX = 0;
    /** 可选输出目录参数下标。 */
    private static final int OUTPUT_ARG_INDEX = 1;
    /** 默认调试数据根目录配置项，用于覆盖 datasets 下的默认 person_info 目录。 */
    private static final String DEBUG_DATASET_ROOT_PROPERTY =
            "raha.udf.debug.dataset-root";
    /** 默认调试数据根目录。 */
    private static final String DEFAULT_DATASET_ROOT =
            "datasets/person_info";
    /** 默认建表插数脚本文件名。 */
    private static final String DEFAULT_LOAD_SQL_FILE =
            "person_info_create_insert_202607201851.sql";

    private RahaSqlScriptRunnerApp() {
    }

    /**
     * 命令行入口，用于本地执行 Spark SQL 文件。
     *
     * <p>参数格式：</p>
     * <pre>
     * RahaSqlScriptRunnerApp &lt;sqlFile&gt; [outputDir]
     * </pre>
     *
     * <p>无参数运行时默认执行：</p>
     * <pre>
     * datasets/person_info/sql/person_info_create_insert_202607201851.sql
     * </pre>
     *
     * <p>本地 Maven 调用示例：</p>
     * <pre>
     * mvn -q "-DskipTests" exec:java "-Dexec.mainClass=com.fiberhome.ml.raha.app.RahaSqlScriptRunnerApp" "-Dexec.classpathScope=test"
     * </pre>
     *
     * @param args 命令行参数；为空时使用默认人员信息建表插数脚本
     * @throws Exception SQL 文件读取或执行失败时抛出
     */
    public static void main(String[] args) throws Exception {
        String[] resolvedArgs = args == null || args.length == 0
                ? defaultLoadDataArgs() : args;
        if (args == null || args.length == 0) {
            System.out.println("RAHA_SQL_DEFAULT_ARGS="
                    + Arrays.toString(resolvedArgs));
        }
        if (resolvedArgs.length < 1) {
            throw new IllegalArgumentException(
                    "用法：RahaSqlScriptRunnerApp <sqlFile> [outputDir]");
        }
        Path scriptPath = Paths.get(resolvedArgs[SCRIPT_ARG_INDEX]);
        if (!Files.isRegularFile(scriptPath)) {
            throw new IllegalArgumentException("SQL 文件不存在：" + scriptPath);
        }
        RahaUdfDriverApp.prepareLocalWindowsHadoopNative();
        Path outputDir = resolvedArgs.length > OUTPUT_ARG_INDEX
                ? Paths.get(resolvedArgs[OUTPUT_ARG_INDEX]) : null;
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
            int executed = executeStatements(spark, statements);
            LOGGER.info("SQL 脚本执行完成，scriptPath={}，executed={}",
                    scriptPath, Integer.valueOf(executed));
            writeSummary(outputDir, scriptPath, executed);
        } finally {
            spark.stop();
        }
    }

    /**
     * 构造默认加载人员信息数据的参数。
     *
     * @return 可直接传给 {@link #main(String[])} 的默认参数
     */
    public static String[] defaultLoadDataArgs() {
        Path datasetRoot = defaultDatasetRoot();
        return new String[] {
                datasetRoot.resolve("sql").resolve(DEFAULT_LOAD_SQL_FILE)
                        .toString(),
                datasetRoot.resolve("out").toString()
        };
    }

    /**
     * 执行默认人员信息建表插数脚本。
     *
     * @throws Exception SQL 文件读取或执行失败时抛出
     */
    public static void loadDefaultPersonInfoData() throws Exception {
        main(defaultLoadDataArgs());
    }

    private static int executeStatements(SparkSession spark,
                                         List<String> statements) {
        int executed = 0;
        for (String statement : statements) {
            String trimmed = statement.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            // 本地调试直接使用工程 classpath，不需要执行集群 SQL 中的 ADD JAR。
            if (trimmed.toUpperCase().startsWith("ADD JAR")) {
                LOGGER.info("跳过本地不需要的语句：{}", oneLine(trimmed));
                continue;
            }
            executed++;
            LOGGER.info("执行 SQL 语句 {}/{}：{}", Integer.valueOf(executed),
                    Integer.valueOf(statements.size()), oneLine(trimmed));
            spark.sql(trimmed);
        }
        return executed;
    }

    private static void writeSummary(Path outputDir,
                                     Path scriptPath,
                                     int executed) throws IOException {
        if (outputDir == null) {
            return;
        }
        Files.createDirectories(outputDir);
        Path marker = outputDir.resolve("sql-run-summary.txt");
        String content = "scriptPath=" + scriptPath + System.lineSeparator()
                + "executed=" + executed + System.lineSeparator();
        Files.write(marker, content.getBytes(StandardCharsets.UTF_8));
        System.out.println("RAHA_SQL_RUN_SUMMARY="
                + marker.toAbsolutePath().normalize());
    }

    private static String readSql(Path scriptPath) throws IOException {
        byte[] bytes = Files.readAllBytes(scriptPath);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 按分号切分 SQL 语句，避免把字符串字面量内部的分号误切开。
     *
     * @param sql SQL 原文
     * @return SQL 语句列表
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

    private static Path defaultDatasetRoot() {
        String configured = System.getProperty(DEBUG_DATASET_ROOT_PROPERTY,
                DEFAULT_DATASET_ROOT);
        return Paths.get(configured);
    }

    private static String oneLine(String statement) {
        return statement == null ? "" : statement.replace('\r', ' ')
                .replace('\n', ' ').trim();
    }
}
