package com.fiberhome.ml.raha.observability;

/**
 * 检查日志文本是否直接包含完整敏感值。
 */
public final class SensitiveLogGuard {

    private SensitiveLogGuard() {
    }

    /**
     * 发现完整敏感值时拒绝日志文本。
     *
     * @param message 待检查日志文本
     * @param sensitiveValues 敏感值集合
     */
    public static void requireSafe(String message, Iterable<String> sensitiveValues) {
        if (message == null || sensitiveValues == null) {
            return;
        }
        for (String sensitiveValue : sensitiveValues) {
            if (sensitiveValue != null && !sensitiveValue.isEmpty()
                    && message.contains(sensitiveValue)) {
                throw new IllegalArgumentException("日志文本包含完整敏感值");
            }
        }
    }
}
