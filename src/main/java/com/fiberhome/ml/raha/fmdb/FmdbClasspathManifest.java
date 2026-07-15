package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.util.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * 读取 FMDB 平台唯一 jar 组合及互斥规则。
 */
public final class FmdbClasspathManifest {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbClasspathManifest.class);
    /** 默认 classpath 清单资源。 */
    private static final String DEFAULT_RESOURCE = "raha-fmdb-classpath.properties";
    /** Spark 版本。 */
    private final String sparkVersion;
    /** Scala 二进制版本。 */
    private final String scalaBinaryVersion;
    /** 允许的 jar 来源目录名称。 */
    private final String sourceDirectory;
    /** 明确忽略的目录名称。 */
    private final String ignoredDirectory;
    /** 必需的精确 jar 文件名。 */
    private final Set<String> requiredJars;
    /** 禁止出现的精确 jar 文件名。 */
    private final Set<String> excludedJars;
    /** 按组件名称定义的互斥组。 */
    private final List<Set<String>> mutuallyExclusiveGroups;

    private FmdbClasspathManifest(Properties properties) {
        this.sparkVersion = required(properties, "spark.version");
        this.scalaBinaryVersion = required(properties, "scala.binary.version");
        this.sourceDirectory = required(properties, "source.directory");
        this.ignoredDirectory = required(properties, "ignored.directory");
        this.requiredJars = csvSet(required(properties, "required.jars"));
        this.excludedJars = csvSet(required(properties, "excluded.jars"));
        this.mutuallyExclusiveGroups = exclusiveGroups(
                required(properties, "mutually.exclusive.groups"));
    }

    /**
     * 从工程资源读取默认 FMDB classpath 清单。
     *
     * @return 不可变清单
     */
    public static FmdbClasspathManifest loadDefault() {
        Properties properties = new Properties();
        try (InputStream input = FmdbClasspathManifest.class.getClassLoader()
                .getResourceAsStream(DEFAULT_RESOURCE)) {
            if (input == null) {
                throw new FmdbClasspathException("缺少 FMDB classpath 清单资源");
            }
            properties.load(input);
            return new FmdbClasspathManifest(properties);
        } catch (IOException exception) {
            LOGGER.error("读取 FMDB classpath 清单失败，resource={}",
                    DEFAULT_RESOURCE, exception);
            throw new FmdbClasspathException("读取 FMDB classpath 清单失败："
                    + exception.getClass().getSimpleName());
        }
    }

    private static String required(Properties properties, String key) {
        return ValueUtils.requireNotBlank(properties.getProperty(key),
                "FMDB classpath 配置 " + key);
    }

    private static Set<String> csvSet(String value) {
        Set<String> values = new LinkedHashSet<String>();
        for (String item : value.split(",")) {
            values.add(ValueUtils.requireNotBlank(item, "FMDB classpath 文件名"));
        }
        return Collections.unmodifiableSet(values);
    }

    private static List<Set<String>> exclusiveGroups(String value) {
        List<Set<String>> groups = new ArrayList<Set<String>>();
        for (String groupText : value.split(",")) {
            Set<String> group = new LinkedHashSet<String>();
            for (String component : groupText.split("\\|")) {
                group.add(ValueUtils.requireNotBlank(component, "FMDB 互斥组件"));
            }
            if (group.size() < 2) {
                throw new FmdbClasspathException("FMDB 互斥组至少包含两个组件");
            }
            groups.add(Collections.unmodifiableSet(group));
        }
        return Collections.unmodifiableList(groups);
    }

    public String getSparkVersion() { return sparkVersion; }
    public String getScalaBinaryVersion() { return scalaBinaryVersion; }
    public String getSourceDirectory() { return sourceDirectory; }
    public String getIgnoredDirectory() { return ignoredDirectory; }
    public Set<String> getRequiredJars() { return requiredJars; }
    public Set<String> getExcludedJars() { return excludedJars; }
    public List<Set<String>> getMutuallyExclusiveGroups() {
        return mutuallyExclusiveGroups;
    }
}
