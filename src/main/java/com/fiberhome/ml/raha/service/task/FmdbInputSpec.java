package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 描述最小请求入口使用的 FMDB 表或只读 SQL 输入。
 *
 * <p>基础入口只保存来源和逻辑数据集标识，行身份为空表示由具体任务按照自身规则补齐。
 * 采样默认使用内容哈希，检测默认继承模型集合记录的行身份规则。</p>
 */
public final class FmdbInputSpec {

    /** FMDB 表数据集标识前缀，避免与调用方声明的 SQL 逻辑标识冲突。 */
    private static final String TABLE_DATASET_PREFIX = "fmdb-table:";
    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** FMDB 完整表名或只读 SQL。 */
    private final String inputReference;
    /** 结果和日志使用的稳定逻辑表名。 */
    private final String tableName;
    /** 输入来源类型，仅允许 FMDB 表或 FMDB SQL。 */
    private final DataFormat format;
    /** 可选行身份规则，为空时由任务类型补齐。 */
    private final RowIdentityConfig rowIdentityConfig;
    /** 可选平台快照标识。 */
    private final String snapshotId;
    /** 可选平台数据版本或业务批次版本。 */
    private final String sourceVersion;
    /** 传递给 Spark 数据源的只读选项。 */
    private final Map<String, String> options;
    /** 参与处理的字段白名单。 */
    private final Set<String> includedColumns;
    /** 不参与处理的字段黑名单。 */
    private final Set<String> excludedColumns;
    /** 结果和日志需要脱敏的字段。 */
    private final Set<String> sensitiveColumns;

    public FmdbInputSpec(String datasetId,
                         String inputReference,
                         String tableName,
                         DataFormat format,
                         RowIdentityConfig rowIdentityConfig,
                         String snapshotId,
                         String sourceVersion,
                         Map<String, String> options,
                         Set<String> includedColumns,
                         Set<String> excludedColumns,
                         Set<String> sensitiveColumns) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "FMDB 逻辑数据集标识");
        String reference = ValueUtils.requireNotBlank(
                inputReference, "FMDB 输入引用");
        this.tableName = ValueUtils.requireNotBlank(tableName, "FMDB 逻辑表名");
        if (format != DataFormat.FMDB_TABLE && format != DataFormat.FMDB_SQL) {
            throw new IllegalArgumentException("最小 FMDB 输入只支持表或只读 SQL");
        }
        this.format = format;
        this.inputReference = format == DataFormat.FMDB_SQL
                ? requireReadOnlySql(reference) : reference;
        this.rowIdentityConfig = rowIdentityConfig;
        this.snapshotId = trimToNull(snapshotId);
        this.sourceVersion = trimToNull(sourceVersion);
        this.options = immutableMap(options);
        this.includedColumns = immutableSet(includedColumns);
        this.excludedColumns = immutableSet(excludedColumns);
        this.sensitiveColumns = immutableSet(sensitiveColumns);
        Set<String> conflicts = new LinkedHashSet<String>(this.includedColumns);
        conflicts.retainAll(this.excludedColumns);
        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException("FMDB 字段不能同时位于白名单和黑名单："
                    + conflicts);
        }
    }

    /**
     * 使用完整 FMDB 表名创建最小输入规格。
     *
     * @param tableName 完整表名，建议包含目录和模式
     * @return 尚未指定行身份和版本的表输入
     */
    public static FmdbInputSpec table(String tableName) {
        String validated = ValueUtils.requireNotBlank(tableName, "FMDB 输入表名");
        return new FmdbInputSpec(datasetIdFromTable(validated), validated,
                validated, DataFormat.FMDB_TABLE, null, null, null,
                null, null, null, null);
    }

    /**
     * 使用稳定逻辑标识和只读 SQL 创建最小输入规格。
     *
     * @param datasetId 调用方维护的稳定逻辑标识
     * @param sql 只读 SQL
     * @return 尚未指定行身份和版本的 SQL 输入
     */
    public static FmdbInputSpec sql(String datasetId, String sql) {
        String logicalId = ValueUtils.requireNotBlank(datasetId, "SQL 逻辑数据集标识");
        return new FmdbInputSpec(logicalId, sql, logicalId,
                DataFormat.FMDB_SQL, null, null, null,
                null, null, null, null);
    }

    /**
     * 按完整表名生成稳定逻辑数据集标识。
     *
     * <p>规范化保留目录、模式和表三个层级，并去除各层外部引用符号。物理读取仍使用原始表名，
     * 因此该方法只影响逻辑标识和幂等语义。</p>
     *
     * @param tableName 完整 FMDB 表名
     * @return 带 FMDB 表前缀的规范逻辑标识
     */
    public static String datasetIdFromTable(String tableName) {
        String validated = ValueUtils.requireNotBlank(tableName, "FMDB 输入表名");
        String[] segments = validated.trim().split("\\s*\\.\\s*", -1);
        StringBuilder normalized = new StringBuilder(TABLE_DATASET_PREFIX);
        for (int index = 0; index < segments.length; index++) {
            String segment = unquote(segments[index].trim());
            if (segment.isEmpty()) {
                throw new IllegalArgumentException("FMDB 表名包含空层级：" + tableName);
            }
            if (index > 0) {
                normalized.append('.');
            }
            normalized.append(segment.toLowerCase(Locale.ROOT));
        }
        return normalized.toString();
    }

    /**
     * 创建包含明确行身份规则的输入副本。
     *
     * @param identityConfig 行身份规则
     * @return 新的不可变输入规格
     */
    public FmdbInputSpec withRowIdentity(RowIdentityConfig identityConfig) {
        if (identityConfig == null) {
            throw new IllegalArgumentException("FMDB 行身份规则不能为空");
        }
        return copy(identityConfig, snapshotId, sourceVersion);
    }

    /**
     * 创建包含业务键行身份规则的输入副本。
     *
     * @param rowKeyColumns 单字段或联合业务键
     * @return 新的不可变输入规格
     */
    public FmdbInputSpec withRowKeyColumns(String... rowKeyColumns) {
        return withRowIdentity(RowIdentityConfig.sourceKey(rowKeyColumns));
    }

    /**
     * 创建显式使用全部业务字段内容哈希的输入副本。
     *
     * @return 新的不可变输入规格
     */
    public FmdbInputSpec withContentHashIdentity() {
        return withRowIdentity(RowIdentityConfig.contentHash());
    }

    /**
     * 创建包含平台快照和来源版本的输入副本。
     *
     * @param newSnapshotId 可选平台快照标识
     * @param newSourceVersion 可选平台来源版本
     * @return 新的不可变输入规格
     */
    public FmdbInputSpec withVersion(String newSnapshotId,
                                     String newSourceVersion) {
        return copy(rowIdentityConfig, trimToNull(newSnapshotId),
                trimToNull(newSourceVersion));
    }

    /**
     * 转换为底层数据加载请求。
     *
     * @param fallbackIdentity 规格未明确声明时使用的行身份规则
     * @return 可直接交给数据加载器的请求
     */
    public DataLoadRequest toDataLoadRequest(RowIdentityConfig fallbackIdentity) {
        RowIdentityConfig identity = rowIdentityConfig == null
                ? fallbackIdentity : rowIdentityConfig;
        if (identity == null) {
            throw new IllegalArgumentException("FMDB 数据加载缺少行身份规则");
        }
        return new DataLoadRequest(datasetId, inputReference, tableName,
                identity, format, options, includedColumns, excludedColumns,
                sensitiveColumns, snapshotId, sourceVersion);
    }

    public String getDatasetId() { return datasetId; }
    public String getInputReference() { return inputReference; }
    public String getTableName() { return tableName; }
    public DataFormat getFormat() { return format; }
    public RowIdentityConfig getRowIdentityConfig() { return rowIdentityConfig; }
    public String getSnapshotId() { return snapshotId; }
    public String getSourceVersion() { return sourceVersion; }
    public Map<String, String> getOptions() { return options; }
    public Set<String> getIncludedColumns() { return includedColumns; }
    public Set<String> getExcludedColumns() { return excludedColumns; }
    public Set<String> getSensitiveColumns() { return sensitiveColumns; }

    private FmdbInputSpec copy(RowIdentityConfig identity,
                               String newSnapshotId,
                               String newSourceVersion) {
        return new FmdbInputSpec(datasetId, inputReference, tableName, format,
                identity, newSnapshotId, newSourceVersion, options,
                includedColumns, excludedColumns, sensitiveColumns);
    }

    private static String unquote(String value) {
        if (value.length() >= 2) {
            char first = value.charAt(0);
            char last = value.charAt(value.length() - 1);
            if ((first == '`' && last == '`')
                    || (first == '"' && last == '"')) {
                return value.substring(1, value.length() - 1).trim();
            }
        }
        return value;
    }

    private static String requireReadOnlySql(String sql) {
        String value = ValueUtils.requireNotBlank(sql, "FMDB 只读 SQL");
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        if (!normalized.startsWith("select ")
                && !normalized.startsWith("with ")) {
            throw new IllegalArgumentException("FMDB SQL 只允许 SELECT 或 WITH 查询");
        }
        return value;
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static Map<String, String> immutableMap(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(values));
    }

    private static Set<String> immutableSet(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new LinkedHashSet<String>();
        for (String value : values) {
            result.add(ValueUtils.requireNotBlank(value, "FMDB 字段名称"));
        }
        return Collections.unmodifiableSet(result);
    }
}
