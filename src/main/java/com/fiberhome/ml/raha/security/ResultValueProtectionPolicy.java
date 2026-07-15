package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.util.ValueProtectionUtils;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 按敏感字段配置控制检测结果展示值，仅哈希模式不会落库任何可读值。
 */
public final class ResultValueProtectionPolicy {

    /** 是否将所有检测字段视为敏感字段。 */
    private final boolean allColumnsSensitive;
    /** 明确配置的敏感字段名称。 */
    private final Set<String> sensitiveColumns;
    /** 敏感字段落库模式。 */
    private final ResultValueMode mode;
    /** 二次脱敏时保留的前缀长度。 */
    private final int visiblePrefix;
    /** 二次脱敏时保留的后缀长度。 */
    private final int visibleSuffix;

    public ResultValueProtectionPolicy(boolean allColumnsSensitive,
                                       Set<String> sensitiveColumns,
                                       ResultValueMode mode,
                                       int visiblePrefix,
                                       int visibleSuffix) {
        if (mode == null || visiblePrefix < 0 || visibleSuffix < 0) {
            throw new IllegalArgumentException("结果值保护模式和可见长度必须有效");
        }
        this.allColumnsSensitive = allColumnsSensitive;
        this.sensitiveColumns = sensitiveColumns == null
                ? Collections.<String>emptySet()
                : Collections.unmodifiableSet(new LinkedHashSet<String>(sensitiveColumns));
        this.mode = mode;
        this.visiblePrefix = visiblePrefix;
        this.visibleSuffix = visibleSuffix;
    }

    public static ResultValueProtectionPolicy preserveExistingMaskedValue() {
        return new ResultValueProtectionPolicy(false,
                Collections.<String>emptySet(), ResultValueMode.MASKED, 0, 0);
    }

    public static ResultValueProtectionPolicy hashOnlyForAllColumns() {
        return new ResultValueProtectionPolicy(true,
                Collections.<String>emptySet(), ResultValueMode.HASH_ONLY, 0, 0);
    }

    /**
     * 返回允许落库的展示值，原始值不在本策略对象中传播。
     */
    public String protectedMaskedValue(DetectionResult result) {
        if (result == null) {
            throw new IllegalArgumentException("检测结果不能为空");
        }
        String columnName = result.getCoordinate().getColumnName();
        boolean sensitive = allColumnsSensitive || sensitiveColumns.contains(columnName);
        if (!sensitive) {
            return result.getMaskedValue();
        }
        if (mode == ResultValueMode.HASH_ONLY || result.getMaskedValue() == null) {
            return null;
        }
        // 对上游展示值再次脱敏，防止调用方误将完整原值写入 maskedValue 字段。
        return ValueProtectionUtils.mask(
                result.getMaskedValue(), visiblePrefix, visibleSuffix);
    }

    public ResultValueMode getMode() { return mode; }
}
