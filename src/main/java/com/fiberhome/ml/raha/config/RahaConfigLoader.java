package com.fiberhome.ml.raha.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;

/**
 * 按类路径默认值、外部文件和系统属性顺序加载 Raha 配置。
 */
public final class RahaConfigLoader {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RahaConfigLoader.class);
    /** 默认类路径配置文件。 */
    public static final String DEFAULT_RESOURCE = "raha-defaults.properties";
    /** 指定外部覆盖文件的系统属性。 */
    public static final String EXTERNAL_FILE_PROPERTY = "raha.config.file";
    /** 指定外部覆盖文件的环境变量。 */
    public static final String EXTERNAL_FILE_ENV = "RAHA_CONFIG_FILE";
    /** 可由系统属性直接覆盖的业务配置前缀。 */
    private static final String BUSINESS_PREFIX = "raha.";
    /** 用于读取类路径资源的类加载器。 */
    private final ClassLoader classLoader;

    public RahaConfigLoader() {
        this(RahaConfigLoader.class.getClassLoader());
    }

    RahaConfigLoader(ClassLoader classLoader) {
        if (classLoader == null) {
            throw new IllegalArgumentException("配置类加载器不能为空");
        }
        this.classLoader = classLoader;
    }

    /**
     * 加载默认资源，并自动合并部署指定的外部文件和系统属性。
     */
    public RahaProperties load() {
        String externalPath = trimToNull(System.getProperty(EXTERNAL_FILE_PROPERTY));
        if (externalPath == null) {
            externalPath = trimToNull(System.getenv(EXTERNAL_FILE_ENV));
        }
        return externalPath == null
                ? loadInternal(null) : loadInternal(Paths.get(externalPath));
    }

    /**
     * 加载默认资源并合并明确指定的外部覆盖文件，主要用于部署装配和测试。
     */
    public RahaProperties load(Path externalFile) {
        if (externalFile == null) {
            throw new IllegalArgumentException("外部配置文件路径不能为空");
        }
        return loadInternal(externalFile);
    }

    private RahaProperties loadInternal(Path externalFile) {
        Properties merged = loadClasspathDefaults();
        Set<String> knownKeys = new LinkedHashSet<String>(
                merged.stringPropertyNames());
        if (externalFile != null) {
            mergeExternal(merged, externalFile);
        }
        mergeSystemProperties(merged);
        validateKnownKeys(merged, knownKeys);
        LOGGER.info("Raha 配置加载完成，propertyCount={}，externalFile={}",
                merged.size(), externalFile == null ? null : externalFile.toAbsolutePath());
        return new RahaProperties(merged);
    }

    private Properties loadClasspathDefaults() {
        InputStream stream = classLoader.getResourceAsStream(DEFAULT_RESOURCE);
        if (stream == null) {
            throw new RahaConfigurationException(null,
                    "类路径默认配置不存在：" + DEFAULT_RESOURCE);
        }
        try (Reader reader = new BufferedReader(new InputStreamReader(
                stream, StandardCharsets.UTF_8))) {
            Properties properties = new Properties();
            properties.load(reader);
            return properties;
        } catch (IOException exception) {
            LOGGER.error("读取类路径默认配置失败，resource={}",
                    DEFAULT_RESOURCE, exception);
            throw new RahaConfigurationException(null,
                    "读取类路径默认配置失败：" + DEFAULT_RESOURCE, exception);
        }
    }

    private static void mergeExternal(Properties target, Path externalFile) {
        Path absolute = externalFile.toAbsolutePath().normalize();
        LOGGER.info("开始读取 Raha 外部覆盖配置，path={}", absolute);
        try (Reader reader = Files.newBufferedReader(
                absolute, StandardCharsets.UTF_8)) {
            Properties overrides = new Properties();
            overrides.load(reader);
            target.putAll(overrides);
        } catch (IOException exception) {
            LOGGER.error("读取 Raha 外部覆盖配置失败，path={}", absolute, exception);
            throw new RahaConfigurationException(null,
                    "读取 Raha 外部覆盖配置失败：" + absolute, exception);
        }
    }

    private static void mergeSystemProperties(Properties target) {
        Properties system = System.getProperties();
        for (String name : system.stringPropertyNames()) {
            // 配置文件路径只控制加载来源，不作为业务配置进入工厂。
            if (name.startsWith(BUSINESS_PREFIX)
                    && !EXTERNAL_FILE_PROPERTY.equals(name)) {
                target.setProperty(name, system.getProperty(name));
            }
        }
    }

    private static void validateKnownKeys(Properties merged,
                                          Set<String> knownKeys) {
        for (String key : merged.stringPropertyNames()) {
            if (!knownKeys.contains(key)) {
                throw new RahaConfigurationException(key,
                        "配置包含未声明的键，propertyKey=" + key);
            }
        }
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
