package com.fiberhome.ml.raha.output.publish;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 负责生成 Raha UDF ZIP，并发布到 HDFS、远端 Web 或本地 Web 降级目录。
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
        String url = publishWeb(sqlContext, zipPath, config);
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

    private String publishWeb(SQLContext sqlContext,
                              Path zipPath,
                              RahaPublishConfig config) throws IOException {
        try {
            String remoteUrl = tryRemoteWeb(sqlContext, zipPath);
            if (!isBlank(remoteUrl)) {
                return remoteUrl;
            }
        } catch (Exception exception) {
            // 异常处理：远端 Web 发布依赖集群 SSH/SCP，失败后降级到本地 Web 目录。
            LOGGER.warn("Raha UDF 远端 Web 发布失败，降级到本地 Web 目录，zipPath={}",
                    zipPath.toAbsolutePath(), exception);
        }
        return publishLocal(zipPath, config);
    }

    private String tryRemoteWeb(SQLContext sqlContext, Path zipPath)
            throws Exception {
        if (sqlContext == null) {
            return "";
        }
        String hostConf = sqlContext.getConf("spark.driver.host");
        if (isBlank(hostConf) || isLocalHost(hostConf)) {
            LOGGER.debug("Raha UDF 当前环境跳过远端 Web 发布，sparkDriverHost={}",
                    hostConf);
            return "";
        }
        LocalDate today = LocalDate.now();
        String fmdbManagerPod = hostConf.replaceFirst(
                "sql-(analysis|query)-([0-9]*)", "fmdbmanager-master-0");
        if (!isSafeRemoteHost(fmdbManagerPod)) {
            LOGGER.warn("Raha UDF 远端 Web 主机名包含非法字符，跳过远端发布，host={}",
                    fmdbManagerPod);
            return "";
        }
        String webPath = "/opt/software/fmdb/manager/web/jetty/webapps/fmdb_report/"
                + today.getYear() + "/" + today.getMonthValue()
                + "/" + today.getDayOfMonth();
        LOGGER.info("Raha UDF 开始远端 Web 发布，host={}，webPath={}，zipPath={}",
                fmdbManagerPod, webPath, zipPath.toAbsolutePath());
        runCommand("ssh", "-p", "22034", fmdbManagerPod,
                "mkdir", "-p", webPath);
        runCommand("scp", "-P", "22034",
                zipPath.toAbsolutePath().toString(),
                fmdbManagerPod + ":" + webPath);
        return "http://" + fmdbManagerPod + ":10030/fmdb_report/"
                + today.getYear() + "/" + today.getMonthValue()
                + "/" + today.getDayOfMonth() + "/"
                + zipPath.getFileName();
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

    private void runCommand(String... command)
            throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true).start();
        try (InputStream inputStream = process.getInputStream()) {
            while (inputStream.read() >= 0) {
                // 外部命令输出不进入返回结果，仅用于避免缓冲区阻塞。
            }
        }
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("命令执行失败 exitCode=" + exitCode
                    + " command=" + Arrays.toString(command));
        }
    }

    private boolean isSafeRemoteHost(String host) {
        return !isBlank(host) && host.matches("[A-Za-z0-9._-]+");
    }

    private boolean isLocalHost(String host) {
        if (isBlank(host)) {
            return true;
        }
        String normalized = host.trim().toLowerCase(Locale.ROOT);
        if ("localhost".equals(normalized)
                || "127.0.0.1".equals(normalized)
                || "0.0.0.0".equals(normalized)) {
            return true;
        }
        try {
            String localHostName = InetAddress.getLocalHost().getHostName();
            return normalized.equalsIgnoreCase(localHostName);
        } catch (Exception exception) {
            return false;
        }
    }
}
