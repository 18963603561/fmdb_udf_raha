package com.fiberhome.ml.raha.security;

/**
 * 定义敏感检测结果允许落库的值表现形式。
 */
public enum ResultValueMode {
    /** 仅保存稳定哈希，脱敏展示值置空。 */
    HASH_ONLY,
    /** 保存稳定哈希和经过二次处理的脱敏展示值。 */
    MASKED
}
