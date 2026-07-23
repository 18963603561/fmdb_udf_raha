package com.fiberhome.ml.raha.annotation.auto;

import java.util.Locale;

/**
 * 定义自动标注批次失败后对采集主流程的影响策略。
 */
public enum AutoAnnotationFailPolicy {

    /** 自动标注失败时保留原始模板并继续完成采集。 */
    WARN_ONLY,
    /** 保留成功批次，失败行留空并交由人工复核。 */
    PARTIAL,
    /** 任意批次失败时终止采集。 */
    FAIL;

    /**
     * 解析外部配置值。
     *
     * @param value 配置文本
     * @return 失败策略
     */
    public static AutoAnnotationFailPolicy parse(String value) {
        String normalized = value == null || value.trim().isEmpty()
                ? WARN_ONLY.name() : value.trim().toUpperCase(Locale.ROOT);
        try {
            return valueOf(normalized);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException(
                    "autoLabelFailPolicy 只支持 WARN_ONLY、PARTIAL 或 FAIL",
                    exception);
        }
    }
}
