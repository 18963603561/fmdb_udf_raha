package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import com.fiberhome.ml.raha.config.core.RahaConfigurationException;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.core.RahaProperties;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * 统一管理 FMDB 持久化总开关、九张物理表开关和列级产物开关。
 */
public final class FmdbPersistenceConfig {

    /** 默认建表 SQL 的类路径资源。 */
    private static final String DEFAULT_SCHEMA_RESOURCE =
            "db/fmdb/raha-fmdb-schema.sql";
    /** 全局 FMDB 表写入模式配置键。 */
    private static final String WRITE_MODE_KEY = "raha.persistence.write-mode";
    /** 快照检查点 HDFS 明细根目录配置键。 */
    private static final String SNAPSHOT_CHECKPOINT_DETAIL_BASE_PATH_KEY =
            "raha.persistence.snapshot-checkpoint.detail-base-path";
    /** 快照检查点逻辑列批大小配置键。 */
    private static final String SNAPSHOT_CHECKPOINT_COLUMN_BATCH_SIZE_KEY =
            "raha.persistence.snapshot-checkpoint.column-batch-size";
    /** 快照检查点 ORC 文件分区数配置键。 */
    private static final String SNAPSHOT_CHECKPOINT_ORC_PARTITION_COUNT_KEY =
            "raha.persistence.snapshot-checkpoint.orc-partition-count";
    /** 快照检查点重明细保留时间配置键。 */
    private static final String SNAPSHOT_CHECKPOINT_RETENTION_MILLIS_KEY =
            "raha.persistence.snapshot-checkpoint.retention-millis";
    /** 快照检查点重明细清理开关配置键。 */
    private static final String SNAPSHOT_CHECKPOINT_CLEANUP_ENABLED_KEY =
            "raha.persistence.snapshot-checkpoint.cleanup-enabled";
    /** 是否启用 FMDB 物理表持久化。 */
    private final boolean enabled;
    /** 是否自动创建不存在的默认表。 */
    private final boolean autoCreateTables;
    /** 自动建表 SQL 的类路径资源。 */
    private final String schemaResource;
    /** 九张物理表各自的入库开关。 */
    private final Map<FmdbPhysicalTable, Boolean> tableSwitches;
    /** 训练列级产物表内各类 JSON 产物的入库开关。 */
    private final Map<FmdbColumnArtifact, Boolean> columnArtifactSwitches;
    /** FMDB 标准物理表的全局追加写入模式。 */
    private final FmdbWriteMode writeMode;
    /** 快照检查点 HDFS 明细根目录。 */
    private final String snapshotCheckpointDetailBasePath;
    /** 快照检查点逻辑列批大小。 */
    private final int snapshotCheckpointColumnBatchSize;
    /** 每个检查点列批的 ORC 文件分区数。 */
    private final int snapshotCheckpointOrcPartitionCount;
    /** 已完成检查点重明细保留时间。 */
    private final long snapshotCheckpointRetentionMillis;
    /** 是否允许清理过期检查点重明细。 */
    private final boolean snapshotCheckpointCleanupEnabled;

    private FmdbPersistenceConfig(boolean enabled,
                                  boolean autoCreateTables,
                                  String schemaResource,
                                  Map<FmdbPhysicalTable, Boolean> tableSwitches,
                                  Map<FmdbColumnArtifact, Boolean> columnArtifactSwitches,
                                  FmdbWriteMode writeMode,
                                  String detailBasePath,
                                  int columnBatchSize,
                                  int orcPartitionCount,
                                  long retentionMillis,
                                  boolean cleanupEnabled) {
        this.enabled = enabled;
        this.autoCreateTables = autoCreateTables;
        this.schemaResource = ValueUtils.requireNotBlank(
                schemaResource, "FMDB 建表脚本资源");
        this.tableSwitches = immutableTableSwitches(tableSwitches);
        this.columnArtifactSwitches = immutableColumnArtifactSwitches(
                columnArtifactSwitches);
        if (writeMode == null) {
            throw new IllegalArgumentException("FMDB 写入模式不能为空");
        }
        this.writeMode = writeMode;
        if (columnBatchSize <= 0 || orcPartitionCount <= 0
                || retentionMillis < 0L) {
            throw new IllegalArgumentException(
                    "快照检查点列批、ORC 分区和保留时间配置非法");
        }
        this.snapshotCheckpointDetailBasePath = ValueUtils.requireNotBlank(
                detailBasePath, "快照检查点 HDFS 明细根目录");
        this.snapshotCheckpointColumnBatchSize = columnBatchSize;
        this.snapshotCheckpointOrcPartitionCount = orcPartitionCount;
        this.snapshotCheckpointRetentionMillis = retentionMillis;
        this.snapshotCheckpointCleanupEnabled = cleanupEnabled;
        validateDependencies();
    }

    /**
     * 从项目默认配置和部署覆盖配置创建持久化配置。
     *
     * @return FMDB 持久化配置
     */
    public static FmdbPersistenceConfig fromDefaults() {
        return fromProperties(RahaDefaultConfigProvider.properties());
    }

    /**
     * 从指定属性创建持久化配置，供部署装配和测试使用。
     *
     * @param properties 已完成默认值合并和类型校验的属性
     * @return FMDB 持久化配置
     */
    public static FmdbPersistenceConfig fromProperties(RahaProperties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("FMDB 持久化属性不能为空");
        }
        EnumMap<FmdbPhysicalTable, Boolean> tableValues =
                new EnumMap<FmdbPhysicalTable, Boolean>(FmdbPhysicalTable.class);
        for (FmdbPhysicalTable table : FmdbPhysicalTable.values()) {
            tableValues.put(table, properties.getBoolean(table.getConfigKey()));
        }
        EnumMap<FmdbColumnArtifact, Boolean> artifactValues =
                new EnumMap<FmdbColumnArtifact, Boolean>(FmdbColumnArtifact.class);
        for (FmdbColumnArtifact artifact : FmdbColumnArtifact.values()) {
            artifactValues.put(artifact,
                    properties.getBoolean(artifact.getConfigKey()));
        }
        return new FmdbPersistenceConfig(
                properties.getBoolean("raha.persistence.enabled"),
                properties.getBoolean("raha.persistence.schema.auto-create"),
                properties.getRequired("raha.persistence.schema.resource"),
                tableValues, artifactValues,
                properties.getEnum(WRITE_MODE_KEY, FmdbWriteMode.class),
                properties.getRequired(SNAPSHOT_CHECKPOINT_DETAIL_BASE_PATH_KEY),
                properties.getInt(SNAPSHOT_CHECKPOINT_COLUMN_BATCH_SIZE_KEY),
                properties.getInt(SNAPSHOT_CHECKPOINT_ORC_PARTITION_COUNT_KEY),
                properties.getLong(SNAPSHOT_CHECKPOINT_RETENTION_MILLIS_KEY),
                properties.getBoolean(SNAPSHOT_CHECKPOINT_CLEANUP_ENABLED_KEY));
    }

    /**
     * 创建便于显式组装和测试的配置构建器。
     *
     * @return 使用推荐默认值的构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 判断指定物理表是否应写入。
     *
     * @param table 目标物理表
     * @return 总开关和表开关均开启时返回真
     */
    public boolean shouldPersist(FmdbPhysicalTable table) {
        requireTable(table);
        return enabled && tableSwitches.get(table);
    }

    /**
     * 判断指定列级 JSON 产物是否应写入。
     *
     * @param artifact 列级产物类型
     * @return 总开关、列级产物表和产物开关均开启时返回真
     */
    public boolean shouldPersist(FmdbColumnArtifact artifact) {
        requireColumnArtifact(artifact);
        return shouldPersist(FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT)
                && columnArtifactSwitches.get(artifact);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isAutoCreateTables() {
        return autoCreateTables;
    }

    public String getSchemaResource() {
        return schemaResource;
    }

    public boolean isTableEnabled(FmdbPhysicalTable table) {
        requireTable(table);
        return tableSwitches.get(table);
    }

    public boolean isColumnArtifactEnabled(FmdbColumnArtifact artifact) {
        requireColumnArtifact(artifact);
        return columnArtifactSwitches.get(artifact);
    }

    public FmdbWriteMode getWriteMode() {
        return writeMode;
    }

    public boolean isDirectAppend() {
        return writeMode == FmdbWriteMode.DIRECT_APPEND;
    }

    public String getSnapshotCheckpointDetailBasePath() {
        return snapshotCheckpointDetailBasePath;
    }

    public int getSnapshotCheckpointColumnBatchSize() {
        return snapshotCheckpointColumnBatchSize;
    }

    public int getSnapshotCheckpointOrcPartitionCount() {
        return snapshotCheckpointOrcPartitionCount;
    }

    public long getSnapshotCheckpointRetentionMillis() {
        return snapshotCheckpointRetentionMillis;
    }

    public boolean isSnapshotCheckpointCleanupEnabled() {
        return snapshotCheckpointCleanupEnabled;
    }

    private void validateDependencies() {
        // 总开关关闭用于紧急停写，此时不要求逐项调整表依赖。
        if (!enabled) {
            return;
        }
        requireTableDependency(FmdbPhysicalTable.ANNOTATION_RECORD,
                FmdbPhysicalTable.SAMPLE_RECORD);
        requireTableDependency(FmdbPhysicalTable.JOB_STAGE_ATTEMPT,
                FmdbPhysicalTable.JOB_RUN);
    }

    private void requireTableDependency(FmdbPhysicalTable source,
                                        FmdbPhysicalTable dependency) {
        // 开启上游产物时必须保留其长期加载或关联所需的依赖表。
        if (tableSwitches.get(source) && !tableSwitches.get(dependency)) {
            throw dependencyException(source.getConfigKey(),
                    dependency.getConfigKey());
        }
    }

    private void requireArtifactDependency(FmdbPhysicalTable source,
                                           FmdbColumnArtifact dependency) {
        // 模型和训练明细只保存字典版本，必须同时保留对应列级定义。
        if (tableSwitches.get(source)
                && !columnArtifactSwitches.get(dependency)) {
            throw dependencyException(source.getConfigKey(),
                    dependency.getConfigKey());
        }
    }

    private static RahaConfigurationException dependencyException(
            String sourceKey, String dependencyKey) {
        return new RahaConfigurationException(sourceKey,
                "持久化配置依赖未开启，propertyKey=" + sourceKey
                        + "，requiredPropertyKey=" + dependencyKey);
    }

    private static Map<FmdbPhysicalTable, Boolean> immutableTableSwitches(
            Map<FmdbPhysicalTable, Boolean> source) {
        if (source == null) {
            throw new IllegalArgumentException("FMDB 物理表开关不能为空");
        }
        EnumMap<FmdbPhysicalTable, Boolean> copy =
                new EnumMap<FmdbPhysicalTable, Boolean>(FmdbPhysicalTable.class);
        for (FmdbPhysicalTable table : FmdbPhysicalTable.values()) {
            Boolean value = source.get(table);
            if (value == null) {
                throw new IllegalArgumentException("缺少 FMDB 物理表开关："
                        + table.getConfigKey());
            }
            copy.put(table, value);
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Map<FmdbColumnArtifact, Boolean> immutableColumnArtifactSwitches(
            Map<FmdbColumnArtifact, Boolean> source) {
        if (source == null) {
            throw new IllegalArgumentException("FMDB 列级产物开关不能为空");
        }
        EnumMap<FmdbColumnArtifact, Boolean> copy =
                new EnumMap<FmdbColumnArtifact, Boolean>(FmdbColumnArtifact.class);
        for (FmdbColumnArtifact artifact : FmdbColumnArtifact.values()) {
            Boolean value = source.get(artifact);
            if (value == null) {
                throw new IllegalArgumentException("缺少 FMDB 列级产物开关："
                        + artifact.getConfigKey());
            }
            copy.put(artifact, value);
        }
        return Collections.unmodifiableMap(copy);
    }

    private static void requireTable(FmdbPhysicalTable table) {
        if (table == null) {
            throw new IllegalArgumentException("FMDB 物理表类型不能为空");
        }
    }

    private static void requireColumnArtifact(FmdbColumnArtifact artifact) {
        if (artifact == null) {
            throw new IllegalArgumentException("FMDB 列级产物类型不能为空");
        }
    }

    /**
     * 用于程序化组装持久化配置，默认值与默认配置文件保持一致。
     */
    public static final class Builder {

        /** 是否启用 FMDB 持久化。 */
        private boolean enabled = true;
        /** 是否自动初始化默认表。 */
        private boolean autoCreateTables = true;
        /** 默认建表脚本资源。 */
        private String schemaResource = DEFAULT_SCHEMA_RESOURCE;
        /** 构建中的物理表开关。 */
        private final EnumMap<FmdbPhysicalTable, Boolean> tableSwitches =
                new EnumMap<FmdbPhysicalTable, Boolean>(FmdbPhysicalTable.class);
        /** 构建中的列级产物开关。 */
        private final EnumMap<FmdbColumnArtifact, Boolean> columnArtifactSwitches =
                new EnumMap<FmdbColumnArtifact, Boolean>(FmdbColumnArtifact.class);
        /** FMDB 标准物理表的全局追加写入模式。 */
        private FmdbWriteMode writeMode = FmdbWriteMode.DIRECT_APPEND;
        /** 快照检查点 HDFS 明细根目录。 */
        private String snapshotCheckpointDetailBasePath =
                "/fmdb/raha/checkpoint";
        /** 快照检查点逻辑列批大小。 */
        private int snapshotCheckpointColumnBatchSize = 10;
        /** 每个列批写出的 ORC 文件分区数。 */
        private int snapshotCheckpointOrcPartitionCount = 4;
        /** 已完成检查点重明细保留时间。 */
        private long snapshotCheckpointRetentionMillis = 604800000L;
        /** 是否允许清理过期重明细。 */
        private boolean snapshotCheckpointCleanupEnabled = true;

        private Builder() {
            for (FmdbPhysicalTable table : FmdbPhysicalTable.values()) {
                tableSwitches.put(table, true);
            }
            tableSwitches.put(FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT, false);
            tableSwitches.put(FmdbPhysicalTable.TRAINING_CELL, false);
            tableSwitches.put(FmdbPhysicalTable.TRAINING_EXAMPLE, false);
            for (FmdbColumnArtifact artifact : FmdbColumnArtifact.values()) {
                columnArtifactSwitches.put(artifact, true);
            }
            columnArtifactSwitches.put(FmdbColumnArtifact.CLUSTER_SUMMARY, false);
            columnArtifactSwitches.put(
                    FmdbColumnArtifact.PROPAGATION_SUMMARY, false);
        }

        /**
         * 设置 FMDB 持久化总开关。
         *
         * @param value 是否允许任何标准物理表写入
         * @return 当前构建器
         */
        public Builder enabled(boolean value) {
            this.enabled = value;
            return this;
        }

        /**
         * 设置是否自动执行默认建表脚本。
         *
         * @param value 是否自动建表
         * @return 当前构建器
         */
        public Builder autoCreateTables(boolean value) {
            this.autoCreateTables = value;
            return this;
        }

        /**
         * 设置默认建表脚本资源。
         *
         * @param value 类路径 SQL 资源
         * @return 当前构建器
         */
        public Builder schemaResource(String value) {
            this.schemaResource = value;
            return this;
        }

        /**
         * 覆盖指定物理表的入库开关。
         *
         * @param table 目标物理表
         * @param value 是否允许入库
         * @return 当前构建器
         */
        public Builder table(FmdbPhysicalTable table, boolean value) {
            requireTable(table);
            tableSwitches.put(table, value);
            return this;
        }

        /**
         * 覆盖指定列级 JSON 产物的入库开关。
         *
         * @param artifact 列级产物
         * @param value 是否允许入库
         * @return 当前构建器
         */
        public Builder columnArtifact(FmdbColumnArtifact artifact,
                                      boolean value) {
            requireColumnArtifact(artifact);
            columnArtifactSwitches.put(artifact, value);
            return this;
        }

        /**
         * 设置 FMDB 标准物理表的全局写入模式。
         *
         * @param value 写入模式
         * @return 当前构建器
         */
        public Builder writeMode(FmdbWriteMode value) {
            if (value == null) {
                throw new IllegalArgumentException("FMDB 写入模式不能为空");
            }
            this.writeMode = value;
            return this;
        }

        /** 设置快照检查点 HDFS 明细根目录。 */
        public Builder snapshotCheckpointDetailBasePath(String value) {
            this.snapshotCheckpointDetailBasePath = value;
            return this;
        }

        /** 设置快照检查点逻辑列批大小。 */
        public Builder snapshotCheckpointColumnBatchSize(int value) {
            this.snapshotCheckpointColumnBatchSize = value;
            return this;
        }

        /** 设置每个检查点列批的 ORC 文件分区数。 */
        public Builder snapshotCheckpointOrcPartitionCount(int value) {
            this.snapshotCheckpointOrcPartitionCount = value;
            return this;
        }

        /** 设置已完成检查点重明细保留时间。 */
        public Builder snapshotCheckpointRetentionMillis(long value) {
            this.snapshotCheckpointRetentionMillis = value;
            return this;
        }

        /** 设置是否允许清理过期检查点重明细。 */
        public Builder snapshotCheckpointCleanupEnabled(boolean value) {
            this.snapshotCheckpointCleanupEnabled = value;
            return this;
        }

        /**
         * 校验依赖并生成不可变配置。
         *
         * @return 完整持久化配置
         */
        public FmdbPersistenceConfig build() {
            return new FmdbPersistenceConfig(enabled, autoCreateTables,
                    schemaResource, tableSwitches, columnArtifactSwitches,
                    writeMode, snapshotCheckpointDetailBasePath,
                    snapshotCheckpointColumnBatchSize,
                    snapshotCheckpointOrcPartitionCount,
                    snapshotCheckpointRetentionMillis,
                    snapshotCheckpointCleanupEnabled);
        }
    }
}
