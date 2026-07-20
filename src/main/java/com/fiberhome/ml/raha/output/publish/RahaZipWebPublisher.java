package com.fiberhome.ml.raha.output.publish;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 负责生成 Raha UDF ZIP，并发布到 HDFS 或本地 Web 降级目录。
 */
public final class RahaZipWebPublisher {

    /** 日志记录器。 */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RahaZipWebPublisher.class);

    public String publish(SQLContext sqlContext,
                          List<RahaZipEntrySource> files,
                          Path workDir,
                          String zipFileName,
                          RahaPublishConfig config) throws IOException {
        if (files == null || workDir == null || zipFileName == null
                || config == null) {
            throw new IllegalArgumentException("ZIP 发布参数不能为空");
        }
        Files.createDirectories(workDir);
        Path zipPath = workDir.resolve(zipFileName).toAbsolutePath().normalize();
        zip(zipPath, files);
        uploadHdfs(sqlContext, zipPath, config);
        String url = publishLocal(zipPath, config);
        LOGGER.info("Raha UDF ZIP 发布完成，zipPath={}，url={}",
                zipPath, url);
        return url;
    }

    private void zip(Path zipPath, List<RahaZipEntrySource> files)
            throws IOException {
        if (zipPath.getParent() != null) {
            Files.createDirectories(zipPath.getParent());
        }
        Map<String, Integer> entryNames = new HashMap<String, Integer>();
        try (ZipOutputStream output =
                     new ZipOutputStream(Files.newOutputStream(zipPath))) {
            for (RahaZipEntrySource source : files) {
                if (source == null || !Files.isRegularFile(source.getSourcePath())) {
                    continue;
                }
                String entryName = uniqueEntryName(
                        source.getEntryName(), entryNames);
                output.putNextEntry(new ZipEntry(entryName));
                try (InputStream input =
                             Files.newInputStream(source.getSourcePath())) {
                    byte[] buffer = new byte[8192];
                    int length;
                    while ((length = input.read(buffer)) >= 0) {
                        output.write(buffer, 0, length);
                    }
                }
                output.closeEntry();
            }
        }
    }

    private void uploadHdfs(SQLContext sqlContext,
                            Path zipPath,
                            RahaPublishConfig config) {
        if (sqlContext == null || isBlank(config.getHdfsExportPath())) {
            return;
        }
        try {
            org.apache.hadoop.conf.Configuration hadoopConfig =
                    sqlContext.sparkContext().hadoopConfiguration();
            String defaultFs = hadoopConfig.get("fs.defaultFS", "file:///");
            if (defaultFs == null || defaultFs.startsWith("file:")) {
                LOGGER.info("当前 Spark 使用本地文件系统，跳过 HDFS 上传，zipPath={}",
                        zipPath);
                return;
            }
            FileSystem fileSystem = FileSystem.get(hadoopConfig);
            org.apache.hadoop.fs.Path targetDir =
                    new org.apache.hadoop.fs.Path(config.getHdfsExportPath());
            if (!fileSystem.exists(targetDir)) {
                fileSystem.mkdirs(targetDir);
            }
            org.apache.hadoop.fs.Path targetFile =
                    new org.apache.hadoop.fs.Path(targetDir,
                            zipPath.getFileName().toString());
            fileSystem.copyFromLocalFile(false, true,
                    new org.apache.hadoop.fs.Path(zipPath.toUri()), targetFile);
            LOGGER.info("Raha UDF ZIP 已上传 HDFS，hdfsPath={}", targetFile);
        } catch (Exception exception) {
            // HDFS 发布失败不阻断本地 Web 降级发布，避免本地验证环境没有 HDFS 时整体失败。
            LOGGER.warn("Raha UDF ZIP 上传 HDFS 失败，继续使用本地发布，zipPath={}",
                    zipPath, exception);
        }
    }

    private String publishLocal(Path zipPath,
                                RahaPublishConfig config) throws IOException {
        LocalDate today = LocalDate.now();
        Path targetDir = config.getLocalWebRoot()
                .resolve(String.valueOf(today.getYear()))
                .resolve(String.valueOf(today.getMonthValue()))
                .resolve(String.valueOf(today.getDayOfMonth()));
        Files.createDirectories(targetDir);
        Path target = targetDir.resolve(zipPath.getFileName().toString());
        Files.copy(zipPath, target, StandardCopyOption.REPLACE_EXISTING);
        if (isBlank(config.getWebBaseUrl())) {
            return target.toAbsolutePath().toString();
        }
        String base = config.getWebBaseUrl();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + "/" + today.getYear() + "/"
                + today.getMonthValue() + "/" + today.getDayOfMonth()
                + "/" + zipPath.getFileName();
    }

    private static String uniqueEntryName(String entryName,
                                          Map<String, Integer> entryNames) {
        String safeName = entryName == null || entryName.trim().isEmpty()
                ? "unknown-file" : entryName.trim().replace('\\', '/');
        Integer count = entryNames.get(safeName);
        if (count == null) {
            entryNames.put(safeName, Integer.valueOf(1));
            return safeName;
        }
        entryNames.put(safeName, Integer.valueOf(count.intValue() + 1));
        int slash = safeName.lastIndexOf('/');
        String dir = slash >= 0 ? safeName.substring(0, slash + 1) : "";
        String file = slash >= 0 ? safeName.substring(slash + 1) : safeName;
        int dot = file.lastIndexOf('.');
        if (dot <= 0) {
            return dir + file + "-" + count;
        }
        return dir + file.substring(0, dot) + "-" + count
                + file.substring(dot);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
