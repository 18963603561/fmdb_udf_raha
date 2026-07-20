package com.fiberhome.ml.raha.output.publish;

import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

/**
 * 保存 Raha UDF 产物发布所需的本地目录、HDFS 目录和 Web 基础地址。
 */
public final class RahaPublishConfig {

    /** 本地工作目录，用于生成 Excel、摘要文件和 ZIP。 */
    private final Path workDir;
    /** 本地 Web 降级发布根目录。 */
    private final Path localWebRoot;
    /** 可选 HDFS 导出目录。 */
    private final String hdfsExportPath;
    /** 可选 Web 下载基础地址。 */
    private final String webBaseUrl;

    public RahaPublishConfig(Path workDir,
                             Path localWebRoot,
                             String hdfsExportPath,
                             String webBaseUrl) {
        if (workDir == null || localWebRoot == null) {
            throw new IllegalArgumentException("Raha 发布目录不能为空");
        }
        this.workDir = workDir.toAbsolutePath().normalize();
        this.localWebRoot = localWebRoot.toAbsolutePath().normalize();
        this.hdfsExportPath = trimToNull(hdfsExportPath);
        this.webBaseUrl = trimToNull(webBaseUrl);
    }

    public static RahaPublishConfig from(Map<String, String> requestValues) {
        String workDir = firstValue(requestValues, "artifactBaseDir",
                "raha.output.work-dir",
                Paths.get(System.getProperty("java.io.tmpdir"),
                        "raha-udf-output").toString());
        String localWebRoot = firstValue(requestValues, "localWebRoot",
                "raha.output.local-web-root",
                Paths.get(System.getProperty("java.io.tmpdir"),
                        "raha-udf-web").toString());
        String hdfsExportPath = firstValue(requestValues, "hdfsExportPath",
                "raha.output.hdfs-export-path",
                "/fmdb/detection/output/");
        String webBaseUrl = firstValue(requestValues, "webBaseUrl",
                "raha.output.web-base-url", null);
        return new RahaPublishConfig(Paths.get(workDir),
                Paths.get(localWebRoot), hdfsExportPath, webBaseUrl);
    }

    public Path getWorkDir() {
        return workDir;
    }

    public Path getLocalWebRoot() {
        return localWebRoot;
    }

    public String getHdfsExportPath() {
        return hdfsExportPath;
    }

    public String getWebBaseUrl() {
        return webBaseUrl;
    }

    private static String firstValue(Map<String, String> requestValues,
                                     String requestKey,
                                     String propertyKey,
                                     String defaultValue) {
        String requestValue = requestValues == null ? null
                : trimToNull(requestValues.get(requestKey));
        if (requestValue != null) {
            return requestValue;
        }
        String propertyValue = trimToNull(
                RahaDefaultConfigProvider.properties().asMap().get(propertyKey));
        return propertyValue == null ? defaultValue : propertyValue;
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }
}
