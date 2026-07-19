package com.fiberhome.ml.raha.data.loader.identity;

import com.fiberhome.ml.raha.config.core.ConfigTextUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 描述业务键、内容哈希算法和规范化协议版本。
 */
public final class RowIdentityConfig {

    /** 首个可用版本采用的规范行序列化协议。 */
    public static final String NORMALIZATION_VERSION = "raha-row-v1";
    /** 行身份模式。 */
    private final RowIdentityMode mode;
    /** 按用户声明顺序保存的单字段或联合键字段。 */
    private final List<String> keyColumns;
    /** 内容指纹算法。 */
    private final RowFingerprintAlgorithm fingerprintAlgorithm;
    /** 规范行序列化协议版本。 */
    private final String normalizationVersion;

    public RowIdentityConfig(RowIdentityMode mode,
                             List<String> keyColumns,
                             RowFingerprintAlgorithm fingerprintAlgorithm,
                             String normalizationVersion) {
        if (mode == null || fingerprintAlgorithm == null) {
            throw new IllegalArgumentException("行身份模式和哈希算法不能为空");
        }
        this.mode = mode;
        this.keyColumns = immutableColumns(keyColumns);
        this.fingerprintAlgorithm = fingerprintAlgorithm;
        this.normalizationVersion = ValueUtils.requireNotBlank(
                normalizationVersion, "行身份规范版本");
        // 当前代码只实现一个明确协议，未知版本必须在任务启动前失败。
        if (!NORMALIZATION_VERSION.equals(this.normalizationVersion)) {
            throw new IllegalArgumentException("不支持的行身份规范版本："
                    + this.normalizationVersion);
        }
        // 业务键模式必须声明字段；内容模式不得携带无效键配置。
        if (mode == RowIdentityMode.SOURCE_KEY && this.keyColumns.isEmpty()) {
            throw new IllegalArgumentException("业务键行身份必须声明至少一个字段");
        }
        if (mode == RowIdentityMode.CONTENT_HASH && !this.keyColumns.isEmpty()) {
            throw new IllegalArgumentException("内容哈希行身份不能声明业务键字段");
        }
    }

    /**
     * 创建单字段或联合键行身份配置。
     *
     * @param keyColumns 按业务约定顺序提供的唯一键字段
     * @return 使用 SHA-256 和当前规范版本的配置
     */
    public static RowIdentityConfig sourceKey(String... keyColumns) {
        return new RowIdentityConfig(RowIdentityMode.SOURCE_KEY,
                keyColumns == null ? Collections.<String>emptyList()
                        : Arrays.asList(keyColumns),
                RowFingerprintAlgorithm.SHA_256, NORMALIZATION_VERSION);
    }

    /**
     * 创建全部业务字段内容哈希行身份配置。
     *
     * @return 使用 SHA-256 和当前规范版本的配置
     */
    public static RowIdentityConfig contentHash() {
        return new RowIdentityConfig(RowIdentityMode.CONTENT_HASH,
                Collections.<String>emptyList(),
                RowFingerprintAlgorithm.SHA_256, NORMALIZATION_VERSION);
    }

    public RowIdentityMode getMode() {
        return mode;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
    }

    public RowFingerprintAlgorithm getFingerprintAlgorithm() {
        return fingerprintAlgorithm;
    }

    public String getNormalizationVersion() {
        return normalizationVersion;
    }

    /**
     * 生成参与任务幂等版本的稳定文本。
     *
     * @return 不依赖集合迭代顺序的规范文本
     */
    public String toCanonicalString() {
        StringBuilder text = new StringBuilder();
        text.append(ConfigTextUtils.token(mode));
        for (String keyColumn : keyColumns) {
            text.append(ConfigTextUtils.token(keyColumn));
        }
        text.append(ConfigTextUtils.token(fingerprintAlgorithm));
        text.append(ConfigTextUtils.token(normalizationVersion));
        return text.toString();
    }

    private static List<String> immutableColumns(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> columns = new ArrayList<String>(values.size());
        Set<String> unique = new LinkedHashSet<String>();
        for (String value : values) {
            String column = ValueUtils.requireNotBlank(value, "业务键字段");
            if (!unique.add(column)) {
                throw new IllegalArgumentException("业务键字段不能重复：" + column);
            }
            columns.add(column);
        }
        return Collections.unmodifiableList(columns);
    }
}
