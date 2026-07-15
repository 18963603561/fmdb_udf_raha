package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.util.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 在部署启动前校验 FMDB jar 是否符合唯一版本和目录规则。
 */
public final class FmdbClasspathValidator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbClasspathValidator.class);
    /** 当前工程确认的 classpath 清单。 */
    private final FmdbClasspathManifest manifest;

    public FmdbClasspathValidator(FmdbClasspathManifest manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("FMDB classpath 清单不能为空");
        }
        this.manifest = manifest;
    }

    /**
     * 校验平台提供的 jar 路径并返回稳定排序后的文件名。
     *
     * @param jarPaths 平台 classpath 中的 jar 路径
     * @return 已校验的稳定文件名列表
     */
    public List<String> validate(Collection<Path> jarPaths) {
        if (jarPaths == null) {
            throw new IllegalArgumentException("FMDB jar 路径集合不能为空");
        }
        Set<String> fileNames = new LinkedHashSet<String>();
        Map<String, List<String>> versionsByComponent =
                new LinkedHashMap<String, List<String>>();
        for (Path path : jarPaths) {
            if (path == null || path.getFileName() == null) {
                throw new FmdbClasspathException("FMDB jar 路径不能包含空值");
            }
            String fileName = ValueUtils.requireNotBlank(
                    path.getFileName().toString(), "FMDB jar 文件名");
            if (!fileNames.add(fileName)) {
                throw new FmdbClasspathException("FMDB classpath 包含重复文件：" + fileName);
            }
            String lower = fileName.toLowerCase(Locale.ROOT);
            if (lower.contains("_2.13") || lower.matches("spark-[^-]+_2\\.12-4\\..*")) {
                throw new FmdbClasspathException("FMDB classpath 混入不兼容 Spark 或 Scala jar："
                        + fileName);
            }
            String component = componentName(fileName);
            if (!versionsByComponent.containsKey(component)) {
                versionsByComponent.put(component, new ArrayList<String>());
            }
            versionsByComponent.get(component).add(fileName);
        }
        for (Map.Entry<String, List<String>> entry : versionsByComponent.entrySet()) {
            if (entry.getValue().size() > 1) {
                throw new FmdbClasspathException("FMDB 组件存在多个版本："
                        + entry.getKey() + "=" + entry.getValue());
            }
        }
        Set<String> missing = new LinkedHashSet<String>(manifest.getRequiredJars());
        missing.removeAll(fileNames);
        if (!missing.isEmpty()) {
            throw new FmdbClasspathException("FMDB classpath 缺少必需 jar：" + missing);
        }
        Set<String> excluded = new LinkedHashSet<String>(manifest.getExcludedJars());
        excluded.retainAll(fileNames);
        if (!excluded.isEmpty()) {
            throw new FmdbClasspathException("FMDB classpath 包含禁止 jar：" + excluded);
        }
        validateExclusiveGroups(fileNames);
        List<String> validated = new ArrayList<String>(fileNames);
        Collections.sort(validated);
        LOGGER.info("FMDB classpath 校验通过，jarCount={}，sparkVersion={}，scalaVersion={}",
                validated.size(), manifest.getSparkVersion(),
                manifest.getScalaBinaryVersion());
        return Collections.unmodifiableList(validated);
    }

    private void validateExclusiveGroups(Set<String> fileNames) {
        for (Set<String> group : manifest.getMutuallyExclusiveGroups()) {
            Set<String> matched = new LinkedHashSet<String>();
            for (String component : group) {
                for (String fileName : fileNames) {
                    if (fileName.startsWith(component + "-")) {
                        matched.add(component);
                    }
                }
            }
            if (matched.size() > 1) {
                throw new FmdbClasspathException("FMDB classpath 同时包含互斥组件：" + matched);
            }
        }
    }

    private static String componentName(String fileName) {
        String withoutExtension = fileName.endsWith(".jar")
                ? fileName.substring(0, fileName.length() - 4) : fileName;
        return withoutExtension.replaceFirst(
                "-(?:V)?[0-9]+(?:\\.[0-9]+)*(?:-[A-Za-z0-9.]+)?$", "");
    }
}
