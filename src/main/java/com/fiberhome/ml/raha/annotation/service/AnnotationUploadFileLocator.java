package com.fiberhome.ml.raha.annotation.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在标注上传目录中定位指定采样批次的最新 Excel 文件。
 *
 * <p>文件名规则与采集阶段保持一致：{@code raha-annotation_安全化采样批次_时间.xls}。
 * 训练阶段只匹配该规则，不兼容旧的原始采样批次文件名。</p>
 */
public final class AnnotationUploadFileLocator {

    /** 日志记录器。 */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AnnotationUploadFileLocator.class);
    /** 标注 Excel 文件名前缀。 */
    private static final String ANNOTATION_FILE_PREFIX = "raha-annotation_";
    /** 采集 ZIP 文件名前缀。 */
    private static final String COLLECT_ZIP_PREFIX = "raha-collect_";
    /** Excel 文件后缀。 */
    private static final String XLS_SUFFIX = ".xls";
    /** ZIP 文件后缀。 */
    private static final String ZIP_SUFFIX = ".zip";

    /**
     * 查找指定采样批次对应的最新标注 Excel。
     *
     * @param spark Spark 会话，用于判断和访问 HDFS
     * @param annotationDir 标注上传目录，可为本地目录或 HDFS 目录
     * @param sampleBatchId 采样批次标识
     * @param localWorkDir HDFS 文件下载后的本地工作目录
     * @return 找到时返回文件定位结果，否则返回空
     */
    public Optional<LocatedAnnotationFile> findLatest(SparkSession spark,
                                                      String annotationDir,
                                                      String sampleBatchId,
                                                      Path localWorkDir) {
        if (annotationDir == null || annotationDir.trim().isEmpty()
                || sampleBatchId == null || sampleBatchId.trim().isEmpty()
                || localWorkDir == null) {
            throw new IllegalArgumentException("标注文件定位参数不能为空");
        }
        try {
            Files.createDirectories(localWorkDir);
            if (shouldUseHdfs(spark, annotationDir)) {
                return findLatestHdfs(spark, annotationDir, sampleBatchId,
                        localWorkDir);
            }
            return findLatestLocal(annotationDir, sampleBatchId, localWorkDir);
        } catch (IOException exception) {
            throw new IllegalStateException("定位标注上传文件失败", exception);
        }
    }

    /**
     * 根据采样批次和文件时间生成采集阶段输出的标注 Excel 文件名。
     *
     * @param sampleBatchId 采样批次标识
     * @param fileTime 文件时间片段
     * @return 标注 Excel 文件名
     */
    public static String annotationExcelName(String sampleBatchId,
                                             String fileTime) {
        return ANNOTATION_FILE_PREFIX
                + annotationOutputToken(sampleBatchId, fileTime) + XLS_SUFFIX;
    }

    /**
     * 根据采样批次和文件时间生成采集阶段输出的 ZIP 文件名。
     *
     * @param sampleBatchId 采样批次标识
     * @param fileTime 文件时间片段
     * @return 采集 ZIP 文件名
     */
    public static String collectZipName(String sampleBatchId,
                                        String fileTime) {
        return COLLECT_ZIP_PREFIX + annotationOutputToken(sampleBatchId,
                fileTime) + ZIP_SUFFIX;
    }

    /**
     * 判断文件名是否匹配指定采样批次。
     *
     * @param fileName 文件名
     * @param sampleBatchId 采样批次标识
     * @return 匹配返回 true
     */
    public static boolean matches(String fileName, String sampleBatchId) {
        if (fileName == null || sampleBatchId == null
                || sampleBatchId.trim().isEmpty()) {
            return false;
        }
        String lower = fileName.toLowerCase(Locale.ROOT);
        if (!lower.endsWith(XLS_SUFFIX) || lower.contains("validation")
                || lower.startsWith(".") || lower.startsWith("_")) {
            return false;
        }
        String expectedPrefix = ANNOTATION_FILE_PREFIX
                + safeSampleBatchToken(sampleBatchId) + "_";
        return fileName.startsWith(expectedPrefix);
    }

    private Optional<LocatedAnnotationFile> findLatestHdfs(SparkSession spark,
                                                           String annotationDir,
                                                           String sampleBatchId,
                                                           Path localWorkDir)
            throws IOException {
        org.apache.hadoop.conf.Configuration configuration =
                spark.sparkContext().hadoopConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        org.apache.hadoop.fs.Path dir =
                new org.apache.hadoop.fs.Path(annotationDir);
        if (!fileSystem.exists(dir)) {
            LOGGER.info("标注上传 HDFS 目录不存在，annotationDir={}",
                    annotationDir);
            return Optional.empty();
        }
        List<FileStatus> candidates = new ArrayList<FileStatus>();
        for (FileStatus status : fileSystem.listStatus(dir)) {
            if (status != null && status.isFile()
                    && matches(status.getPath().getName(), sampleBatchId)) {
                candidates.add(status);
            }
        }
        if (candidates.isEmpty()) {
            LOGGER.info("未找到匹配 sampleBatchId 的 HDFS 标注文件，sampleBatchId={}，annotationDir={}",
                    sampleBatchId, annotationDir);
            return Optional.empty();
        }
        Collections.sort(candidates, hdfsComparator());
        FileStatus selected = candidates.get(0);
        Path localTarget = localWorkDir.resolve(
                safeLocalName(selected.getPath().getName()));
        fileSystem.copyToLocalFile(false, selected.getPath(),
                new org.apache.hadoop.fs.Path(localTarget.toUri()), true);
        LOGGER.info("已定位最新 HDFS 标注文件，sampleBatchId={}，sourcePath={}，localPath={}",
                sampleBatchId, selected.getPath(), localTarget);
        return Optional.of(new LocatedAnnotationFile(
                selected.getPath().getName(), selected.getPath().toString(),
                localTarget, selected.getModificationTime()));
    }

    private Optional<LocatedAnnotationFile> findLatestLocal(String annotationDir,
                                                            String sampleBatchId,
                                                            Path localWorkDir)
            throws IOException {
        Path dir = Paths.get(annotationDir).toAbsolutePath().normalize();
        if (!Files.isDirectory(dir)) {
            LOGGER.info("标注上传本地目录不存在，annotationDir={}", dir);
            return Optional.empty();
        }
        List<Path> candidates = new ArrayList<Path>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> matches(path.getFileName().toString(),
                            sampleBatchId))
                    .forEach(candidates::add);
        }
        if (candidates.isEmpty()) {
            LOGGER.info("未找到匹配 sampleBatchId 的本地标注文件，sampleBatchId={}，annotationDir={}",
                    sampleBatchId, dir);
            return Optional.empty();
        }
        Collections.sort(candidates, localComparator());
        Path selected = candidates.get(0);
        Path localTarget = localWorkDir.resolve(
                safeLocalName(selected.getFileName().toString()));
        if (!selected.equals(localTarget)) {
            Files.copy(selected, localTarget,
                    StandardCopyOption.REPLACE_EXISTING);
        }
        long modifiedAt = Files.getLastModifiedTime(selected).toMillis();
        LOGGER.info("已定位最新本地标注文件，sampleBatchId={}，sourcePath={}，localPath={}",
                sampleBatchId, selected, localTarget);
        return Optional.of(new LocatedAnnotationFile(
                selected.getFileName().toString(), selected.toString(),
                localTarget, modifiedAt));
    }

    private static boolean shouldUseHdfs(SparkSession spark,
                                         String annotationDir) {
        if (annotationDir.startsWith("hdfs:")) {
            return true;
        }
        if (spark == null) {
            return false;
        }
        try {
            String defaultFs = spark.sparkContext().hadoopConfiguration()
                    .get("fs.defaultFS", "file:///");
            return defaultFs != null && !defaultFs.startsWith("file:");
        } catch (Exception exception) {
            return false;
        }
    }

    private static Comparator<FileStatus> hdfsComparator() {
        return new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus first, FileStatus second) {
                int byTime = Long.compare(second.getModificationTime(),
                        first.getModificationTime());
                if (byTime != 0) {
                    return byTime;
                }
                return second.getPath().getName()
                        .compareTo(first.getPath().getName());
            }
        };
    }

    private static Comparator<Path> localComparator() {
        return new Comparator<Path>() {
            @Override
            public int compare(Path first, Path second) {
                try {
                    int byTime = Long.compare(
                            Files.getLastModifiedTime(second).toMillis(),
                            Files.getLastModifiedTime(first).toMillis());
                    if (byTime != 0) {
                        return byTime;
                    }
                    return second.getFileName().toString()
                            .compareTo(first.getFileName().toString());
                } catch (IOException exception) {
                    throw new IllegalStateException("读取标注文件修改时间失败",
                            exception);
                }
            }
        };
    }

    private static String safeLocalName(String fileName) {
        String value = fileName == null ? "annotation.xls" : fileName;
        value = value.replaceAll("[\\\\/:*?\"<>|\\s]+", "_");
        value = value.replaceAll("_+", "_");
        return value.endsWith(XLS_SUFFIX) ? value : value + XLS_SUFFIX;
    }

    private static String safeSampleBatchToken(String sampleBatchId) {
        String text = sampleBatchId == null || sampleBatchId.trim().isEmpty()
                ? "unknown" : sampleBatchId.trim();
        text = text.replaceAll("[\\\\/:*?\"<>|\\s]+", "_");
        text = text.replaceAll("_+", "_");
        text = text.replaceAll("[^a-zA-Z0-9._\\-]", "_");
        return text.isEmpty() ? "unknown" : text;
    }

    private static String annotationOutputToken(String sampleBatchId,
                                                String fileTime) {
        return safeSampleBatchToken(sampleBatchId) + "_"
                + safeTimeToken(fileTime);
    }

    private static String safeTimeToken(String fileTime) {
        String text = fileTime == null || fileTime.trim().isEmpty()
                ? String.valueOf(System.currentTimeMillis()) : fileTime.trim();
        text = text.replaceAll("[^a-zA-Z0-9._\\-]", "_");
        return text.isEmpty() ? String.valueOf(System.currentTimeMillis())
                : text;
    }
}
