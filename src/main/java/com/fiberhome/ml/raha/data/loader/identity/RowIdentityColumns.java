package com.fiberhome.ml.raha.data.loader.identity;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 集中定义加载后加入数据集的技术字段，所有业务哈希必须排除这些字段。
 */
public final class RowIdentityColumns {

    /** 下游统一使用的稳定逻辑行标识字段。 */
    public static final String ROW_ID = "_raha_row_id";
    /** 全部业务字段生成的内容指纹字段。 */
    public static final String ROW_CONTENT_HASH = "_raha_row_content_hash";
    /** 当前逻辑行折叠的物理重复行数量。 */
    public static final String DUPLICATE_COUNT = "_raha_duplicate_count";
    /** 去重窗口使用的临时字段。 */
    static final String REPRESENTATIVE_ORDER = "_raha_representative_order";
    /** 所有保留技术字段。 */
    private static final Set<String> RESERVED = Collections.unmodifiableSet(
            new LinkedHashSet<String>(Arrays.asList(ROW_ID, ROW_CONTENT_HASH,
                    DUPLICATE_COUNT, REPRESENTATIVE_ORDER)));

    private RowIdentityColumns() {
    }

    public static boolean isTechnical(String columnName) {
        return RESERVED.contains(columnName);
    }

    public static Set<String> reserved() {
        return RESERVED;
    }
}
