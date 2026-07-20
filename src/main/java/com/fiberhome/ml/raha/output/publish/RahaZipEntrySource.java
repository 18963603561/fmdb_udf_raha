package com.fiberhome.ml.raha.output.publish;

import java.nio.file.Path;

/**
 * 描述 ZIP 内一个条目的本地来源文件和归档路径。
 */
public final class RahaZipEntrySource {

    /** 本地来源文件。 */
    private final Path sourcePath;
    /** ZIP 内相对路径。 */
    private final String entryName;

    public RahaZipEntrySource(Path sourcePath, String entryName) {
        if (sourcePath == null || entryName == null
                || entryName.trim().isEmpty()) {
            throw new IllegalArgumentException("ZIP 条目来源和名称不能为空");
        }
        this.sourcePath = sourcePath.toAbsolutePath().normalize();
        this.entryName = entryName.replace('\\', '/');
    }

    public Path getSourcePath() {
        return sourcePath;
    }

    public String getEntryName() {
        return entryName;
    }
}
