package com.fiberhome.ml.raha.fmdb;

/**
 * 表示 FMDB 平台 classpath 缺失、版本冲突或来源目录非法。
 */
public final class FmdbClasspathException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public FmdbClasspathException(String message) {
        super(message);
    }
}
