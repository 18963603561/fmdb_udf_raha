package com.fiberhome.ml.raha.data;

/**
 * 数据行身份解析模式。
 */
public enum RowIdentityMode {
    /** 使用调用方提供或元数据解析出的稳定业务键。 */
    KEY,
    /** 使用全部输入列规范内容进行分组。 */
    CONTENT_GROUP
}
