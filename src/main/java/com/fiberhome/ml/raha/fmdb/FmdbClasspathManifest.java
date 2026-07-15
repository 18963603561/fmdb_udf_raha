package com.fiberhome.ml.raha.fmdb;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 定义 FMDB 平台唯一 jar 组合及互斥规则。
 */
public final class FmdbClasspathManifest {

    /** Spark 版本。 */
    private static final String SPARK_VERSION = "3.3.1";
    /** Scala 二进制版本。 */
    private static final String SCALA_BINARY_VERSION = "2.12";
    /** 必需的精确 jar 文件名。 */
    private static final Set<String> REQUIRED_JARS = immutableSet(
            "spark-sql_2.12-3.3.1.jar",
            "spark-catalyst_2.12-3.3.1.jar",
            "spark-mllib_2.12-3.3.1.jar",
            "spark-enhance-2.6.0-SNAPSHOT.jar",
            "spark-enhance-depends-2.6.0-SNAPSHOT.jar",
            "spark-extensions-2.6.0-SNAPSHOT.jar",
            "sql-common-2.3.0-SNAPSHOT.jar",
            "sql-spark-2.3.0-SNAPSHOT.jar",
            "gdk-db3-3.7.0.jar");
    /** 禁止出现的精确 jar 文件名。 */
    private static final Set<String> EXCLUDED_JARS = immutableSet(
            "sql-extended-functions-2.3.0-SNAPSHOT.jar");
    /** 按组件名称定义的互斥组。 */
    private static final List<Set<String>> MUTUALLY_EXCLUSIVE_GROUPS =
            Collections.singletonList(immutableSet(
                    "spark-extensions", "sql-extended-functions"));

    private FmdbClasspathManifest() {
    }

    /**
     * 返回代码中已经确认的 FMDB classpath 清单。
     *
     * @return 不可变清单
     */
    public static FmdbClasspathManifest loadDefault() {
        return new FmdbClasspathManifest();
    }

    private static Set<String> immutableSet(String... values) {
        return Collections.unmodifiableSet(new LinkedHashSet<String>(
                Arrays.asList(values)));
    }

    public String getSparkVersion() { return SPARK_VERSION; }
    public String getScalaBinaryVersion() { return SCALA_BINARY_VERSION; }
    public Set<String> getRequiredJars() { return REQUIRED_JARS; }
    public Set<String> getExcludedJars() { return EXCLUDED_JARS; }
    public List<Set<String>> getMutuallyExclusiveGroups() {
        return MUTUALLY_EXCLUSIVE_GROUPS;
    }
}
