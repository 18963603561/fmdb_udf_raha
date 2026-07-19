package com.fiberhome.ml.raha.repository.adapter.fmdb.schema;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;

/**
 * 读取类路径中的 FMDB 建表脚本并执行幂等初始化。
 */
public final class FmdbSchemaInitializer {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbSchemaInitializer.class);
    /** 执行建表语句的 Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 持久化和建表配置。 */
    private final FmdbPersistenceConfig config;
    /** 用于读取建表资源的类加载器。 */
    private final ClassLoader classLoader;
    /** 当前初始化器是否已经成功执行。 */
    private boolean initialized;

    /**
     * 创建使用默认类加载器的 FMDB 表初始化器。
     *
     * @param sparkSession 执行建表语句的 Spark 会话
     * @param config 持久化和建表配置
     */
    public FmdbSchemaInitializer(SparkSession sparkSession,
                                 FmdbPersistenceConfig config) {
        this(sparkSession, config, FmdbSchemaInitializer.class.getClassLoader());
    }

    FmdbSchemaInitializer(SparkSession sparkSession,
                          FmdbPersistenceConfig config,
                          ClassLoader classLoader) {
        if (sparkSession == null || config == null || classLoader == null) {
            throw new IllegalArgumentException("FMDB 表初始化依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.config = config;
        this.classLoader = classLoader;
    }

    /**
     * 在启用持久化和自动建表时创建所有不存在的默认表。
     */
    public synchronized void initialize() {
        if (initialized) {
            return;
        }
        // 持久化总开关关闭时不得创建任何业务表。
        if (!config.isEnabled()) {
            LOGGER.info("FMDB 持久化已关闭，跳过默认表初始化");
            initialized = true;
            return;
        }
        // 运维手工建表场景只关闭自动初始化，不改变后续业务写入决策。
        if (!config.isAutoCreateTables()) {
            LOGGER.info("FMDB 自动建表已关闭，跳过默认表初始化，schemaResource={}",
                    config.getSchemaResource());
            initialized = true;
            return;
        }
        List<String> statements = parseStatements(readScript(config.getSchemaResource()));
        LOGGER.info("开始初始化 FMDB 默认表，schemaResource={}，statementCount={}",
                config.getSchemaResource(), statements.size());
        for (int index = 0; index < statements.size(); index++) {
            try {
                // 所有 DDL 必须使用 IF NOT EXISTS，保证服务重复初始化不会破坏已有表。
                sparkSession.sql(statements.get(index));
            } catch (RuntimeException exception) {
                LOGGER.error("FMDB 默认表初始化失败，schemaResource={}，statementIndex={}",
                        config.getSchemaResource(), index + 1, exception);
                throw new IllegalStateException("FMDB 默认表初始化失败，语句序号："
                        + (index + 1), exception);
            }
        }
        initialized = true;
        LOGGER.info("FMDB 默认表初始化完成，schemaResource={}，statementCount={}",
                config.getSchemaResource(), statements.size());
    }

    private String readScript(String resource) {
        InputStream stream = classLoader.getResourceAsStream(resource);
        if (stream == null) {
            throw new IllegalStateException("FMDB 建表脚本不存在：" + resource);
        }
        StringBuilder script = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                script.append(line).append('\n');
            }
            return script.toString();
        } catch (IOException exception) {
            LOGGER.error("读取 FMDB 建表脚本失败，schemaResource={}", resource, exception);
            throw new IllegalStateException("读取 FMDB 建表脚本失败：" + resource,
                    exception);
        }
    }

    /**
     * 将 FMDB 建表脚本拆分为可逐条执行的 SQL 语句。
     *
     * @param script 原始建表脚本文本
     * @return 按执行顺序排列的 SQL 语句列表
     */
    public static List<String> parseStatements(String script) {
        if (script == null || script.trim().isEmpty()) {
            throw new IllegalArgumentException("FMDB 建表脚本不能为空");
        }
        List<String> statements = new ArrayList<String>();
        StringBuilder statement = new StringBuilder();
        boolean singleQuoted = false;
        boolean backtickQuoted = false;
        boolean lineComment = false;
        // 只在引号和行注释之外按分号切分，避免破坏表属性中的文本值。
        for (int index = 0; index < script.length(); index++) {
            char current = script.charAt(index);
            char next = index + 1 < script.length() ? script.charAt(index + 1) : '\0';
            if (lineComment) {
                if (current == '\n') {
                    lineComment = false;
                    statement.append(current);
                }
                continue;
            }
            // SQL 行注释不进入最终提交给 Spark 的语句。
            if (!singleQuoted && !backtickQuoted && current == '-' && next == '-') {
                lineComment = true;
                index++;
                continue;
            }
            // 两个连续单引号属于字符串转义，不能切换引号状态。
            if (!backtickQuoted && current == '\'') {
                statement.append(current);
                if (singleQuoted && next == '\'') {
                    statement.append(next);
                    index++;
                } else {
                    singleQuoted = !singleQuoted;
                }
                continue;
            }
            // 反引号中的分号和注释符号属于标识内容。
            if (!singleQuoted && current == '`') {
                statement.append(current);
                if (backtickQuoted && next == '`') {
                    statement.append(next);
                    index++;
                } else {
                    backtickQuoted = !backtickQuoted;
                }
                continue;
            }
            if (!singleQuoted && !backtickQuoted && current == ';') {
                addStatement(statements, statement);
                continue;
            }
            statement.append(current);
        }
        if (singleQuoted || backtickQuoted) {
            throw new IllegalArgumentException("FMDB 建表脚本存在未闭合的引号");
        }
        addStatement(statements, statement);
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("FMDB 建表脚本不包含可执行语句");
        }
        return Collections.unmodifiableList(statements);
    }

    private static void addStatement(List<String> statements,
                                     StringBuilder statement) {
        String sql = statement.toString().trim();
        if (!sql.isEmpty()) {
            statements.add(sql);
        }
        statement.setLength(0);
    }
}
