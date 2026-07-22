package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 描述文件或测试数据的读取方式、字段范围和快照版本。
 */
public final class DataLoadRequest {

    /** 逻辑数据集标识。 */
    private final String datasetId;
    /** 输入文件、表名或 SQL 原文，用于实际读取数据。 */
    private final String inputReference;
    /** 持久化和输出展示的来源引用，SQL 输入时为首个来源表。 */
    private final String sourceReference;
    /** 来源表或逻辑表名称。 */
    private final String tableName;
    /** 输入业务键或全字段内容哈希行身份配置。 */
    private final RowIdentityConfig rowIdentityConfig;
    /** 输入数据格式。 */
    private final DataFormat format;
    /** 传递给 Spark 数据源的读取选项。 */
    private final Map<String, String> options;
    /** 只允许参与检测的字段，为空表示不限制。 */
    private final Set<String> includedColumns;
    /** 禁止参与检测的字段。 */
    private final Set<String> excludedColumns;
    /** 需要在结果和日志中脱敏的字段。 */
    private final Set<String> sensitiveColumns;
    /** 调用方明确指定的快照标识。 */
    private final String snapshotId;
    /** 数据源版本、文件修改时间或业务批次。 */
    private final String sourceVersion;

    public DataLoadRequest(String datasetId,
                           String inputReference,
                           String tableName,
                           RowIdentityConfig rowIdentityConfig,
                           DataFormat format,
                           Map<String, String> options,
                           Set<String> includedColumns,
                           Set<String> excludedColumns,
                           Set<String> sensitiveColumns,
                           String snapshotId,
                           String sourceVersion) {
        this(datasetId, inputReference, inputReference, tableName,
                rowIdentityConfig, format, options, includedColumns,
                excludedColumns, sensitiveColumns, snapshotId, sourceVersion);
    }

    public DataLoadRequest(String datasetId,
                           String inputReference,
                           String sourceReference,
                           String tableName,
                           RowIdentityConfig rowIdentityConfig,
                           DataFormat format,
                           Map<String, String> options,
                           Set<String> includedColumns,
                           Set<String> excludedColumns,
                           Set<String> sensitiveColumns,
                           String snapshotId,
                           String sourceVersion) {
        this.datasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        this.inputReference = ValueUtils.requireNotBlank(inputReference, "输入数据引用");
        this.sourceReference = ValueUtils.requireNotBlank(
                sourceReference, "展示来源引用");
        this.tableName = ValueUtils.requireNotBlank(tableName, "表名");
        if (rowIdentityConfig == null) {
            throw new IllegalArgumentException("行身份配置不能为空");
        }
        this.rowIdentityConfig = rowIdentityConfig;
        if (format == null) {
            throw new IllegalArgumentException("数据格式不能为空");
        }
        this.format = format;
        this.options = immutableMap(options);
        this.includedColumns = immutableSet(includedColumns);
        this.excludedColumns = immutableSet(excludedColumns);
        this.sensitiveColumns = immutableSet(sensitiveColumns);
        Set<String> conflicts = new HashSet<String>(this.includedColumns);
        conflicts.retainAll(this.excludedColumns);
        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException("字段不能同时出现在加载白名单和黑名单中：" + conflicts);
        }
        this.snapshotId = snapshotId;
        this.sourceVersion = sourceVersion;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public String getInputReference() {
        return inputReference;
    }

    public String getSourceReference() {
        return sourceReference;
    }

    public String getTableName() {
        return tableName;
    }

    public RowIdentityConfig getRowIdentityConfig() {
        return rowIdentityConfig;
    }

    public DataFormat getFormat() {
        return format;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public Set<String> getIncludedColumns() {
        return includedColumns;
    }

    public Set<String> getExcludedColumns() {
        return excludedColumns;
    }

    public Set<String> getSensitiveColumns() {
        return sensitiveColumns;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getSourceVersion() {
        return sourceVersion;
    }

    /**
     * 创建只允许指定字段参与检测和训练的请求副本。
     *
     * <p>物理输入和完整模式保持不变，白名单只改变字段的可检测属性，
     * 因而所有列批仍共享同一个模式哈希和行身份规则。</p>
     *
     * @param columns 当前列批字段
     * @return 字段范围受限的新加载请求
     */
    public DataLoadRequest withIncludedColumns(Set<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("列批字段白名单不能为空");
        }
        return new DataLoadRequest(datasetId, inputReference, sourceReference,
                tableName, rowIdentityConfig, format, options, columns,
                excludedColumns, sensitiveColumns, snapshotId, sourceVersion);
    }

    private static Map<String, String> immutableMap(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<String, String>(values));
    }

    private static Set<String> immutableSet(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<String>(values));
    }
}
