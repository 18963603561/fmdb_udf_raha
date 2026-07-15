package com.fiberhome.ml.raha.model;

/**
 * 隔离 Spark MLlib 运行时可用性判断，便于验证降级分支。
 */
public interface MllibAvailability {

    boolean isAvailable();
}
