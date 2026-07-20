package com.fiberhome.ml.raha.annotation.service;

import java.nio.file.Path;

/**
 * 保存训练 UDF 定位到的标注上传文件信息。
 */
public final class LocatedAnnotationFile {

    /** 上传文件名。 */
    private final String fileName;
    /** 上传文件原始路径，可能是 HDFS 路径或本地路径。 */
    private final String sourcePath;
    /** 可供 POI 读取的本地文件路径。 */
    private final Path localPath;
    /** 文件最后修改时间。 */
    private final long modifiedAt;

    public LocatedAnnotationFile(String fileName,
                                 String sourcePath,
                                 Path localPath,
                                 long modifiedAt) {
        if (fileName == null || fileName.trim().isEmpty()
                || sourcePath == null || sourcePath.trim().isEmpty()
                || localPath == null) {
            throw new IllegalArgumentException("标注上传文件信息不能为空");
        }
        this.fileName = fileName;
        this.sourcePath = sourcePath;
        this.localPath = localPath.toAbsolutePath().normalize();
        this.modifiedAt = modifiedAt;
    }

    public String getFileName() {
        return fileName;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public Path getLocalPath() {
        return localPath;
    }

    public long getModifiedAt() {
        return modifiedAt;
    }
}
