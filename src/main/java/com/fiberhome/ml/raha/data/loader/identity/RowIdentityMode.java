package com.fiberhome.ml.raha.data.loader.identity;

/**
 * 定义输入数据生成稳定逻辑行标识的方式。
 */
public enum RowIdentityMode {
    /** 使用用户声明的单字段或联合业务键生成行标识。 */
    SOURCE_KEY,
    /** 没有业务键时使用全部业务字段内容哈希作为行标识。 */
    CONTENT_HASH
}
