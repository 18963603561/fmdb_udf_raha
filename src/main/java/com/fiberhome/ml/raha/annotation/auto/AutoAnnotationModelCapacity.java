package com.fiberhome.ml.raha.annotation.auto;

import java.util.Locale;

/**
 * 根据模型名称提供上下文和最大输出令牌档案，未知模型使用保守回退值。
 */
public final class AutoAnnotationModelCapacity {

    /** 档案名称。 */
    private final String profileName;
    /** 模型上下文窗口令牌数。 */
    private final int contextWindowTokens;
    /** 模型最大输出令牌数。 */
    private final int maxOutputTokens;

    private AutoAnnotationModelCapacity(String profileName,
                                        int contextWindowTokens,
                                        int maxOutputTokens) {
        this.profileName = profileName;
        this.contextWindowTokens = contextWindowTokens;
        this.maxOutputTokens = maxOutputTokens;
    }

    /**
     * 按模型名称解析容量档案。
     *
     * @param model 模型名称或服务端模型标识
     * @return 匹配的容量档案
     */
    public static AutoAnnotationModelCapacity resolve(String model) {
        String value = model == null ? ""
                : model.trim().toLowerCase(Locale.ROOT);
        if (value.contains("qwen3-coder-plus")
                || value.contains("qwen3.5-plus")) {
            return new AutoAnnotationModelCapacity("QWEN_HOSTED_1M",
                    1000000, 65536);
        }
        if (value.contains("qwen3-coder-next")
                || value.contains("qwen3-coder-30b")
                || value.contains("qwen3-coder-480b")
                || value.contains("qwen3.5code")
                || value.contains("qwen3.5-code")
                || value.contains("qwen3.5-coder")) {
            return new AutoAnnotationModelCapacity("QWEN_CODER_256K",
                    262144, 32768);
        }
        if (value.contains("qwen2.5-3b")) {
            return new AutoAnnotationModelCapacity("QWEN_2_5_3B_32K",
                    32768, 8192);
        }
        return new AutoAnnotationModelCapacity("CONSERVATIVE_32K",
                32768, 4096);
    }

    public String getProfileName() { return profileName; }
    public int getContextWindowTokens() { return contextWindowTokens; }
    public int getMaxOutputTokens() { return maxOutputTokens; }
}
