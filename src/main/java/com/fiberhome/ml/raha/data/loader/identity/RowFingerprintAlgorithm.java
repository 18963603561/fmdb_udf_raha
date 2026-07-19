package com.fiberhome.ml.raha.data.loader.identity;

/**
 * 定义逻辑行身份支持的哈希算法。
 */
public enum RowFingerprintAlgorithm {
    /** 使用 JDK 8 必须支持的 SHA-256。 */
    SHA_256("SHA-256");

    /** 对外记录的标准算法名称。 */
    private final String standardName;

    RowFingerprintAlgorithm(String standardName) {
        this.standardName = standardName;
    }

    public String getStandardName() {
        return standardName;
    }
}
