package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
import com.fiberhome.ml.raha.checkpoint.SnapshotCheckpointWriteSession;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringMetrics;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPartitionUtils;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbClusterSummaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbColumnProfileCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbFeatureDictionaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonValue;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbStrategyArtifactCodec;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 FMDB 保存检查点元数据，并按列批将训练必要明细保存为 HDFS ORC。
 *
 * <p>检查点表只承担发现、审计和完整性判断；单元格特征与聚类归属在
 * 同一条 ORC 记录中保存。所有列批成功后才提交最终清单。</p>
 */
public final class FmdbSnapshotCheckpointRepository
        implements SnapshotCheckpointRepository {

    /** 检查点最终清单记录类型。 */
    private static final String MANIFEST = "MANIFEST";
    /** 已清理检查点记录类型。 */
    private static final String CLEANED = "CLEANED";
    /** 字段画像记录类型。 */
    private static final String PROFILE = "PROFILE";
    /** 策略计划记录类型。 */
    private static final String STRATEGY_PLAN = "STRATEGY_PLAN";
    /** 特征字典记录类型。 */
    private static final String FEATURE_DICTIONARY = "FEATURE_DICTIONARY";
    /** 聚类摘要记录类型。 */
    private static final String CLUSTER_SUMMARY = "CLUSTER_SUMMARY";
    /** HDFS 明细列批清单记录类型。 */
    private static final String DETAIL_BATCH = "DETAIL_BATCH";
    /** 检查点级记录作用域。 */
    private static final String SCOPE_CHECKPOINT = "CHECKPOINT";
    /** 列批级记录作用域。 */
    private static final String SCOPE_BATCH = "COLUMN_BATCH";
    /** 列级记录作用域。 */
    private static final String SCOPE_COLUMN = "COLUMN";
    /** 当前 HDFS 明细协议版本。 */
    private static final String DETAIL_SCHEMA_VERSION = "2";

    /** HDFS ORC 明细模式。 */
    private static final StructType DETAIL_SCHEMA = DataTypes.createStructType(
            new StructField[] {
                DataTypes.createStructField("checkpoint_id", DataTypes.StringType, false),
                DataTypes.createStructField("batch_index", DataTypes.IntegerType, false),
                DataTypes.createStructField("column_name", DataTypes.StringType, false),
                DataTypes.createStructField("row_id", DataTypes.StringType, false),
                DataTypes.createStructField("cell_id", DataTypes.StringType, false),
                DataTypes.createStructField("value_hash", DataTypes.StringType, false),
                DataTypes.createStructField("feature_dictionary_version",
                        DataTypes.StringType, false),
                DataTypes.createStructField("feature_values",
                        DataTypes.createMapType(DataTypes.IntegerType,
                                DataTypes.DoubleType, false), false),
                DataTypes.createStructField("cluster_version", DataTypes.StringType, false),
                DataTypes.createStructField("cluster_id", DataTypes.StringType, false)
            });

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbSnapshotCheckpointRepository.class);
    /** 当前 Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** FMDB 持久化配置。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 检查点物理表名。 */
    private final String tableName;
    /** 当前进程尚未提交最终清单的检查点写会话。 */
    private final Map<String, PendingCheckpoint> pendingCheckpoints =
            new LinkedHashMap<String, PendingCheckpoint>();

    public FmdbSnapshotCheckpointRepository(SparkSession sparkSession,
                                            FmdbTableGateway tableGateway,
                                            FmdbPersistenceConfig persistenceConfig) {
        if (sparkSession == null || tableGateway == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 快照检查点仓储依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = FmdbPhysicalTable.SNAPSHOT_CHECKPOINT.getTableName();
    }

    @Override
    public int getColumnBatchSize() {
        return persistenceConfig.getSnapshotCheckpointColumnBatchSize();
    }

    @Override
    public synchronized SnapshotCheckpointWriteSession begin(
            String sourceJobId,
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            List<StrategyPlan> strategyPlans,
            String strategyPlanVersion,
            String configFingerprint,
            long createdAt) {
        String jobId = ValueUtils.requireNotBlank(sourceJobId, "来源任务标识");
        if (dataset == null || snapshot == null || strategyPlans == null
                || createdAt <= 0L) {
            throw new IllegalArgumentException("快照检查点写会话参数不能为空");
        }
        String fingerprint = ValueUtils.requireNotBlank(configFingerprint,
                "执行配置指纹");
        String checkpointId = checkpointId(jobId, snapshot, fingerprint,
                createdAt);
        String rowSetFingerprint = rowSetFingerprint(snapshot);
        SnapshotCheckpointWriteSession session =
                new SnapshotCheckpointWriteSession(checkpointId, jobId,
                        dataset, snapshot, strategyPlans, strategyPlanVersion,
                        fingerprint, createdAt);
        if (!persistenceConfig.shouldPersist(
                FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)) {
            LOGGER.info("FMDB 快照检查点持久化已关闭，创建空写会话，jobId={}，snapshotId={}",
                    jobId, snapshot.getSnapshotId());
            return session;
        }
        if (pendingCheckpoints.containsKey(checkpointId)) {
            throw new IllegalStateException("检查点写会话已经存在：" + checkpointId);
        }
        List<FmdbTableRecord> globalRecords = globalRecords(checkpointId, jobId,
                dataset, snapshot, rowSetFingerprint, fingerprint, createdAt);
        pendingCheckpoints.put(checkpointId, new PendingCheckpoint(
                globalRecords, rowSetFingerprint));
        LOGGER.info("快照检查点分批写会话已创建，checkpointId={}，datasetId={}，"
                        + "snapshotId={}，detailBasePath={}", checkpointId,
                snapshot.getDatasetId(), snapshot.getSnapshotId(),
                persistenceConfig.getSnapshotCheckpointDetailBasePath());
        return session;
    }

    @Override
    public synchronized void saveColumnBatch(
            SnapshotCheckpointWriteSession session,
            int batchIndex,
            List<String> columns,
            FeatureAssemblyResult features,
            ClusteringBatchResult clustering) {
        if (session == null || columns == null || columns.isEmpty()
                || features == null || clustering == null || batchIndex <= 0) {
            throw new IllegalArgumentException("检查点列批参数不能为空");
        }
        if (!persistenceConfig.shouldPersist(
                FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)) {
            return;
        }
        PendingCheckpoint pending = requiredPending(session);
        if (batchIndex != pending.nextBatchIndex) {
            throw new IllegalStateException("检查点列批序号不连续，expected="
                    + pending.nextBatchIndex + "，actual=" + batchIndex);
        }
        Set<String> uniqueColumns = new LinkedHashSet<String>(columns);
        if (uniqueColumns.size() != columns.size()
                || !Collections.disjoint(pending.completedColumns, uniqueColumns)) {
            throw new IllegalArgumentException("检查点列批字段重复：" + columns);
        }
        validateColumnBatch(session, uniqueColumns, features, clustering);
        DetailBatch detail = writeDetailBatch(session.getCheckpointId(),
                batchIndex, session.getSnapshot(), columns,
                featuresByColumn(features.getRows()),
                assignmentsByColumn(clustering));
        Map<String, Object> payload = detailPayload(detail, columns);
        List<FmdbTableRecord> metadata = batchMetadata(
                session.getCheckpointId(), session.getSourceJobId(),
                session.getSnapshot(), features, clustering, columns,
                pending.rowSetFingerprint, session.getConfigFingerprint(),
                session.getCreatedAt(), payload);
        if (batchIndex == 1) {
            metadata.addAll(0, pending.globalRecords);
        }
        append(metadata);
        pending.batchPayloads.add(payload);
        pending.completedColumns.addAll(uniqueColumns);
        pending.detailRecordCount += detail.recordCount;
        pending.nextBatchIndex++;
        LOGGER.info("快照检查点列批提交完成，checkpointId={}，batchIndex={}，"
                        + "columns={}，detailCount={}，detailPath={}",
                session.getCheckpointId(), batchIndex, columns,
                detail.recordCount, detail.path);
    }

    private static void validateColumnBatch(
            SnapshotCheckpointWriteSession session,
            Set<String> columns,
            FeatureAssemblyResult features,
            ClusteringBatchResult clustering) {
        if (!features.getDictionaries().keySet().equals(columns)
                || !clustering.getResults().keySet().equals(columns)) {
            throw new IllegalArgumentException("检查点列批字段、字典和聚类结果不一致");
        }
        Map<String, Long> featureCounts = new LinkedHashMap<String, Long>();
        Map<String, Long> assignmentCounts = new LinkedHashMap<String, Long>();
        for (String column : columns) {
            featureCounts.put(column, Long.valueOf(0L));
            assignmentCounts.put(column, Long.valueOf(clustering.getResults()
                    .get(column).getAssignments().size()));
        }
        for (SparseFeatureRow row : features.getRows()) {
            if (!columns.contains(row.getColumnName())) {
                throw new IllegalArgumentException("检查点列批包含范围外特征："
                        + row.getColumnName());
            }
            FeatureDictionary dictionary = features.getDictionaries().get(
                    row.getColumnName());
            if (!dictionary.getVersion().equals(
                    row.getFeatureDictionaryVersion())) {
                throw new IllegalArgumentException("检查点特征字典版本不一致："
                        + row.getColumnName());
            }
            featureCounts.put(row.getColumnName(), Long.valueOf(
                    featureCounts.get(row.getColumnName()).longValue() + 1L));
        }
        for (String column : columns) {
            long featureCount = featureCounts.get(column).longValue();
            long assignmentCount = assignmentCounts.get(column).longValue();
            if (featureCount != session.getSnapshot().getRowCount()
                    || assignmentCount != featureCount) {
                throw new IllegalArgumentException("检查点列批行数不完整，column="
                        + column + "，snapshotRows="
                        + session.getSnapshot().getRowCount()
                        + "，features=" + featureCount + "，assignments="
                        + assignmentCount);
            }
        }
    }

    @Override
    public synchronized void complete(SnapshotCheckpointWriteSession session,
                                      StrategyBatchResult strategyBatch) {
        if (session == null || strategyBatch == null) {
            throw new IllegalArgumentException("检查点写会话和策略摘要不能为空");
        }
        if (!persistenceConfig.shouldPersist(
                FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)) {
            return;
        }
        PendingCheckpoint pending = requiredPending(session);
        Set<String> expectedColumns = new LinkedHashSet<String>();
        for (ColumnMetadata column : session.getDataset().getColumns()) {
            if (column.isDetectable()) {
                expectedColumns.add(column.getName());
            }
        }
        if (!pending.completedColumns.equals(expectedColumns)) {
            throw new IllegalStateException("检查点字段未完整提交，expected="
                    + expectedColumns + "，actual=" + pending.completedColumns);
        }
        if (pending.batchPayloads.isEmpty()) {
            append(pending.globalRecords);
        }
        Map<String, Object> payload = manifestPayload(session.getDataset(),
                session.getSnapshot(), session.getStrategyPlanVersion(),
                pending.batchPayloads, checkpointRoot(session.getCheckpointId(),
                        session.getSnapshot()));
        List<FmdbTableRecord> finalRecords = new ArrayList<FmdbTableRecord>();
        finalRecords.add(strategyRecord(session, pending.rowSetFingerprint,
                strategyBatch));
        finalRecords.add(record(base(session.getCheckpointId(),
                session.getSourceJobId(), session.getSnapshot(),
                pending.rowSetFingerprint, session.getConfigFingerprint(),
                session.getCreatedAt()), MANIFEST, SCOPE_CHECKPOINT, null,
                payload));
        append(finalRecords);
        pendingCheckpoints.remove(session.getCheckpointId());
        LOGGER.info("HDFS 快照检查点最终清单提交完成，checkpointId={}，"
                        + "columnBatchCount={}，columnCount={}，detailRecordCount={}",
                session.getCheckpointId(), pending.batchPayloads.size(),
                pending.completedColumns.size(), pending.detailRecordCount);
    }

    @Override
    public synchronized void abort(SnapshotCheckpointWriteSession session) {
        if (session == null) {
            return;
        }
        PendingCheckpoint pending = pendingCheckpoints.remove(
                session.getCheckpointId());
        if (pending == null) {
            return;
        }
        deletePathQuietly(checkpointRoot(session.getCheckpointId(),
                session.getSnapshot()));
        LOGGER.warn("快照检查点写会话已中止，checkpointId={}，"
                        + "completedColumnCount={}，detailRecordCount={}",
                session.getCheckpointId(), pending.completedColumns.size(),
                pending.detailRecordCount);
    }

    @Override
    public Optional<SnapshotPreparedArtifacts> restore(
            String datasetId,
            String snapshotId,
            String configFingerprint,
            Set<String> includedColumns) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        String fingerprint = ValueUtils.requireNotBlank(configFingerprint,
                "执行配置指纹");
        Set<String> requestedColumns = normalizedColumns(includedColumns);
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)
                || !tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        List<Row> candidates = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT),
                functions.col("dataset_id").equalTo(dataset)
                        .and(functions.col("snapshot_id").equalTo(snapshot))
                        .and(functions.col("config_fingerprint").equalTo(fingerprint))
                        .and(functions.col("record_type").isin(MANIFEST, CLEANED)))
                .orderBy(functions.col("created_at").desc())
                .collectAsList();
        Row manifest = latestActiveManifest(candidates);
        if (manifest == null) {
            LOGGER.warn("未找到可复用 HDFS 快照检查点，datasetId={}，snapshotId={}",
                    dataset, snapshot);
            return Optional.empty();
        }
        String checkpointId = manifest.getAs("checkpoint_id");
        Map<String, Object> manifestPayload = FmdbJsonCodec.readObject(
                (String) manifest.getAs("payload_json"));
        validateRequestedColumns(columns(manifestPayload), requestedColumns);
        LOGGER.info("开始恢复 HDFS 快照检查点，checkpointId={}，datasetId={}，"
                        + "snapshotId={}，includedColumns={}", checkpointId,
                dataset, snapshot, requestedColumns);
        List<Row> metadataRows = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT),
                functions.col("checkpoint_id").equalTo(checkpointId))
                .collectAsList();
        List<Row> detailRows = readDetailRows(metadataRows, requestedColumns);
        SnapshotPreparedArtifacts artifacts = restoreArtifacts(checkpointId,
                fingerprint, metadataRows, detailRows, requestedColumns);
        LOGGER.info("HDFS 快照检查点恢复完成，checkpointId={}，includedColumns={}，"
                        + "featureRowCount={}，assignmentCount={}", checkpointId,
                requestedColumns, artifacts.getFeatures().getRows().size(),
                artifacts.getClustering().getMetrics().getAssignmentCount());
        return Optional.of(artifacts);
    }

    @Override
    public void cleanupExpired(long currentTimeMillis) {
        if (!persistenceConfig.isSnapshotCheckpointCleanupEnabled()
                || !persistenceConfig.shouldPersist(
                FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)
                || !tableGateway.tableExists(tableName)) {
            return;
        }
        long cutoff = currentTimeMillis
                - persistenceConfig.getSnapshotCheckpointRetentionMillis();
        Column expiredManifest = functions.col("record_type").equalTo(MANIFEST)
                .and(functions.col("created_at").leq(cutoff));
        Column cleanedRecord = functions.col("record_type").equalTo(CLEANED);
        List<Row> rows = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT),
                expiredManifest.or(cleanedRecord))
                .collectAsList();
        Set<String> cleaned = new LinkedHashSet<String>();
        for (Row row : rows) {
            if (CLEANED.equals(row.getAs("record_type"))) {
                cleaned.add((String) row.getAs("checkpoint_id"));
            }
        }
        for (Row row : rows) {
            String checkpointId = row.getAs("checkpoint_id");
            if (!MANIFEST.equals(row.getAs("record_type"))
                    || cleaned.contains(checkpointId)) {
                continue;
            }
            Map<String, Object> payload = FmdbJsonCodec.readObject(
                    (String) row.getAs("payload_json"));
            String root = FmdbJsonValue.requiredText(payload, "detailRootPath");
            deletePath(root);
            append(Collections.singletonList(cleanedRecord(row,
                    currentTimeMillis, root)));
            LOGGER.info("过期快照检查点 HDFS 明细已清理，checkpointId={}，detailRootPath={}",
                    checkpointId, root);
        }
    }

    private SnapshotPreparedArtifacts restoreArtifacts(
            String checkpointId,
            String configFingerprint,
            List<Row> metadataRows,
            List<Row> detailRows,
            Set<String> includedColumns) {
        Row manifest = requiredManifest(metadataRows);
        Map<String, Object> payload = FmdbJsonCodec.readObject(
                (String) manifest.getAs("payload_json"));
        DatasetSnapshot snapshot = snapshot(payload, manifest);
        List<ColumnMetadata> selected = selectedColumns(columns(payload),
                includedColumns);
        RahaDataset dataset = new RahaDataset(snapshot.getDatasetId(),
                snapshot.getSnapshotId(), snapshot.getTableName(),
                snapshot.getRowIdColumn(), selected, null,
                snapshot.getSchemaHash(), profiles(metadataRows, includedColumns));
        StrategyArtifacts strategy = strategyArtifacts(metadataRows,
                includedColumns);
        FeatureAssemblyResult features = features(metadataRows, detailRows,
                dataset, includedColumns);
        ClusteringBatchResult clustering = clustering(metadataRows, detailRows,
                dataset, includedColumns);
        return new SnapshotPreparedArtifacts(checkpointId, dataset, snapshot,
                strategy.plans, strategy.batch, features,
                FmdbJsonValue.requiredText(payload, "strategyPlanVersion"),
                clustering, configFingerprint);
    }

    private DetailBatch writeDetailBatch(
            String checkpointId,
            int batchIndex,
            DatasetSnapshot snapshot,
            List<String> columns,
            Map<String, List<SparseFeatureRow>> featuresByColumn,
            Map<String, Map<String, ClusterAssignment>> assignmentsByColumn) {
        List<Row> rows = new ArrayList<Row>();
        MessageDigest checksum = messageDigest();
        for (String column : columns) {
            Map<String, ClusterAssignment> assignments =
                    assignmentsByColumn.get(column);
            if (assignments == null) {
                throw new IllegalStateException("检查点字段缺少聚类结果：" + column);
            }
            List<SparseFeatureRow> columnRows = featuresByColumn.containsKey(column)
                    ? featuresByColumn.get(column)
                    : Collections.<SparseFeatureRow>emptyList();
            for (SparseFeatureRow feature : columnRows) {
                ClusterAssignment assignment = assignments.get(feature.getCellId());
                if (assignment == null || assignment.getCoordinate() == null) {
                    throw new IllegalStateException("检查点特征缺少聚类归属或坐标："
                            + feature.getCellId());
                }
                rows.add(RowFactory.create(checkpointId, Integer.valueOf(batchIndex),
                        column, assignment.getCoordinate().getRowId(),
                        feature.getCellId(), feature.getValueHash(),
                        feature.getFeatureDictionaryVersion(),
                        feature.getValues(), assignment.getClusterVersion(),
                        assignment.getClusterId()));
                updateChecksum(checksum, feature, assignment);
            }
        }
        String finalPath = batchPath(checkpointId, snapshot, batchIndex);
        String temporaryPath = finalPath + ".tmp-" + System.nanoTime();
        Dataset<Row> frame = sparkSession.createDataFrame(rows, DETAIL_SCHEMA);
        int partitions = Math.min(Math.max(1, rows.size()),
                persistenceConfig.getSnapshotCheckpointOrcPartitionCount());
        try {
            LOGGER.info("开始写入快照检查点 HDFS 列批，checkpointId={}，batchIndex={}，"
                            + "columns={}，recordCount={}，partitionCount={}，temporaryPath={}",
                    checkpointId, batchIndex, columns, rows.size(), partitions,
                    temporaryPath);
            frame.repartition(partitions).write().mode(SaveMode.ErrorIfExists)
                    .format("orc").save(temporaryPath);
            long stored = sparkSession.read().schema(DETAIL_SCHEMA)
                    .format("orc").load(temporaryPath).count();
            if (stored != rows.size()) {
                throw new IllegalStateException("检查点 HDFS 列批写入数量校验失败，expected="
                        + rows.size() + "，actual=" + stored);
            }
            commitPath(temporaryPath, finalPath);
            return new DetailBatch(batchIndex, finalPath, rows.size(),
                    hex(checksum.digest()));
        } catch (RuntimeException exception) {
            deletePathQuietly(temporaryPath);
            LOGGER.error("快照检查点 HDFS 列批写入失败，checkpointId={}，batchIndex={}，"
                            + "temporaryPath={}", checkpointId, batchIndex,
                    temporaryPath, exception);
            throw exception;
        }
    }

    private List<Row> readDetailRows(List<Row> metadataRows,
                                     Set<String> includedColumns) {
        List<DetailReference> references = new ArrayList<DetailReference>();
        for (Row row : metadataRows) {
            if (!DETAIL_BATCH.equals(row.getAs("record_type"))) {
                continue;
            }
            Map<String, Object> payload = FmdbJsonCodec.readObject(
                    (String) row.getAs("payload_json"));
            if (includedColumns.isEmpty()
                    || intersects(stringList(payload, "columns"), includedColumns)) {
                references.add(new DetailReference(
                        FmdbJsonValue.requiredText(payload, "path"),
                        FmdbJsonValue.requiredNumber(payload,
                                "recordCount").longValue()));
            }
        }
        if (references.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            List<String> paths = new ArrayList<String>();
            for (DetailReference reference : references) {
                long stored = sparkSession.read().schema(DETAIL_SCHEMA)
                        .format("orc").load(reference.path).count();
                if (stored != reference.recordCount) {
                    throw new IllegalStateException(
                            "检查点 HDFS 列批数量校验失败，path="
                                    + reference.path + "，expected="
                                    + reference.recordCount + "，actual=" + stored);
                }
                paths.add(reference.path);
            }
            Dataset<Row> frame = sparkSession.read().schema(DETAIL_SCHEMA)
                    .format("orc").load(paths.toArray(new String[0]));
            if (!includedColumns.isEmpty()) {
                frame = frame.filter(functions.col("column_name").isin(
                        includedColumns.toArray(new Object[0])));
            }
            return frame.collectAsList();
        } catch (RuntimeException exception) {
            LOGGER.error("读取快照检查点 HDFS 明细失败，references={}，includedColumns={}",
                    references, includedColumns, exception);
            throw exception;
        }
    }

    private void append(List<FmdbTableRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        List<Row> rows = new ArrayList<Row>(records.size());
        for (FmdbTableRecord record : records) {
            rows.add(record.toRow());
        }
        Dataset<Row> frame = sparkSession.createDataFrame(rows,
                FmdbTableSchemas.schema(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT));
        tableGateway.appendDirect(tableName, frame, records.size());
    }

    private List<FmdbTableRecord> globalRecords(
            String checkpointId,
            String jobId,
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            String rowSetFingerprint,
            String fingerprint,
            long createdAt) {
        List<FmdbTableRecord> records = new ArrayList<FmdbTableRecord>();
        List<ColumnProfile> profiles = new ArrayList<ColumnProfile>(
                dataset.getProfiles().values());
        Collections.sort(profiles, Comparator.comparing(
                ColumnProfile::getColumnName));
        for (ColumnProfile profile : profiles) {
            Map<String, Object> values = base(checkpointId, jobId, snapshot,
                    rowSetFingerprint, fingerprint, createdAt);
            values.put("artifact_version", snapshot.getSnapshotId());
            values.put("profile_json", FmdbColumnProfileCodec.write(profile));
            records.add(record(values, PROFILE, SCOPE_COLUMN,
                    profile.getColumnName(), null));
        }
        return records;
    }

    private static FmdbTableRecord strategyRecord(
            SnapshotCheckpointWriteSession session,
            String rowSetFingerprint,
            StrategyBatchResult strategyBatch) {
        Map<String, Object> values = base(session.getCheckpointId(),
                session.getSourceJobId(), session.getSnapshot(),
                rowSetFingerprint, session.getConfigFingerprint(),
                session.getCreatedAt());
        values.put("artifact_version", session.getStrategyPlanVersion());
        Set<String> executedIds = new LinkedHashSet<String>();
        for (StrategyExecutionResult execution : strategyBatch.getExecutions()) {
            executedIds.add(execution.getSummary().getStrategyId());
        }
        List<StrategyPlan> executedPlans = new ArrayList<StrategyPlan>();
        for (StrategyPlan plan : session.getStrategyPlans()) {
            if (executedIds.contains(plan.getStrategyId())) {
                executedPlans.add(plan);
            }
        }
        if (executedPlans.size() != executedIds.size()) {
            throw new IllegalStateException("检查点策略摘要包含未声明的策略计划");
        }
        values.put("strategy_plan_json", FmdbStrategyArtifactCodec.write(
                executedPlans, summaries(strategyBatch)));
        return record(values, STRATEGY_PLAN, SCOPE_CHECKPOINT, null, null);
    }

    private List<FmdbTableRecord> batchMetadata(
            String checkpointId,
            String jobId,
            DatasetSnapshot snapshot,
            FeatureAssemblyResult features,
            ClusteringBatchResult clustering,
            List<String> columns,
            String rowSetFingerprint,
            String fingerprint,
            long createdAt,
            Map<String, Object> detailPayload) {
        List<FmdbTableRecord> records = new ArrayList<FmdbTableRecord>();
        for (String column : columns) {
            FeatureDictionary dictionary = features.getDictionaries().get(column);
            ColumnClusteringResult cluster = clustering.getResults().get(column);
            if (dictionary == null || cluster == null) {
                throw new IllegalStateException("检查点列批缺少字典或聚类摘要：" + column);
            }
            Map<String, Object> dictionaryValues = base(checkpointId, jobId,
                    snapshot, rowSetFingerprint, fingerprint, createdAt);
            dictionaryValues.put("artifact_version", dictionary.getVersion());
            dictionaryValues.put("feature_dictionary_json",
                    FmdbFeatureDictionaryCodec.write(dictionary));
            records.add(record(dictionaryValues, FEATURE_DICTIONARY,
                    SCOPE_COLUMN, column, null));
            Map<String, Object> clusterValues = base(checkpointId, jobId,
                    snapshot, rowSetFingerprint, fingerprint, createdAt);
            clusterValues.put("artifact_version", cluster.getClusterVersion());
            clusterValues.put("cluster_version", cluster.getClusterVersion());
            clusterValues.put("cluster_summary_json",
                    FmdbClusterSummaryCodec.write(cluster));
            records.add(record(clusterValues, CLUSTER_SUMMARY,
                    SCOPE_COLUMN, column, null));
        }
        records.add(record(base(checkpointId, jobId, snapshot,
                rowSetFingerprint, fingerprint, createdAt), DETAIL_BATCH,
                SCOPE_BATCH, null, detailPayload));
        return records;
    }

    private static Map<String, ColumnProfile> profiles(
            List<Row> rows,
            Set<String> includedColumns) {
        Map<String, ColumnProfile> result =
                new LinkedHashMap<String, ColumnProfile>();
        for (Row row : rows) {
            if (PROFILE.equals(row.getAs("record_type"))) {
                ColumnProfile profile = FmdbColumnProfileCodec.read(
                        (String) row.getAs("profile_json"));
                if (includedColumns.isEmpty()
                        || includedColumns.contains(profile.getColumnName())) {
                    result.put(profile.getColumnName(), profile);
                }
            }
        }
        return result;
    }

    private static StrategyArtifacts strategyArtifacts(
            List<Row> rows,
            Set<String> includedColumns) {
        Map<String, StrategyPlan> plans = new LinkedHashMap<String, StrategyPlan>();
        Map<String, StrategyRunSummary> summaries =
                new LinkedHashMap<String, StrategyRunSummary>();
        for (Row row : rows) {
            if (!STRATEGY_PLAN.equals(row.getAs("record_type"))) {
                continue;
            }
            String json = row.getAs("strategy_plan_json");
            for (StrategyPlan plan : FmdbStrategyArtifactCodec.readPlans(json)) {
                if (includedColumns.isEmpty()
                        || includedColumns.containsAll(plan.getTargetColumns())) {
                    plans.put(plan.getStrategyId(), plan);
                }
            }
            for (StrategyRunSummary summary
                    : FmdbStrategyArtifactCodec.readSummaries(json)) {
                if (plans.containsKey(summary.getStrategyId())) {
                    summaries.put(summary.getStrategyId(), summary);
                }
            }
        }
        List<StrategyExecutionResult> executions =
                new ArrayList<StrategyExecutionResult>();
        for (StrategyRunSummary summary : summaries.values()) {
            executions.add(StrategyExecutionResult.summaryOnly(summary));
        }
        return new StrategyArtifacts(new ArrayList<StrategyPlan>(plans.values()),
                new StrategyBatchResult(executions));
    }

    private static FeatureAssemblyResult features(
            List<Row> metadataRows,
            List<Row> detailRows,
            RahaDataset dataset,
            Set<String> includedColumns) {
        Map<String, FeatureDictionary> dictionaries =
                new LinkedHashMap<String, FeatureDictionary>();
        for (Row row : metadataRows) {
            if (FEATURE_DICTIONARY.equals(row.getAs("record_type"))) {
                FeatureDictionary dictionary = FmdbFeatureDictionaryCodec.read(
                        (String) row.getAs("feature_dictionary_json"));
                if (includedColumns.isEmpty() || includedColumns.contains(
                        dictionary.getColumnName())) {
                    dictionaries.put(dictionary.getColumnName(), dictionary);
                }
            }
        }
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        for (Row row : detailRows) {
            String column = row.getAs("column_name");
            CellCoordinate coordinate = new CellCoordinate(
                    dataset.getDatasetId(), dataset.getSnapshotId(),
                    (String) row.getAs("row_id"), column);
            rows.add(new SparseFeatureRow((String) row.getAs("cell_id"),
                    column, coordinate, (String) row.getAs("value_hash"), null,
                    (String) row.getAs("feature_dictionary_version"),
                    integerDoubleMap(row.getJavaMap(
                            row.fieldIndex("feature_values"))),
                    Collections.<String, String>emptyMap()));
        }
        Collections.sort(rows, Comparator.comparing(SparseFeatureRow::getCellId));
        long retained = 0L;
        for (FeatureDictionary dictionary : dictionaries.values()) {
            retained += dictionary.getDefinitions().size();
        }
        return new FeatureAssemblyResult(dictionaries, rows,
                new FeatureAssemblyMetrics(rows.size(), retained, retained, 0L));
    }

    private static ClusteringBatchResult clustering(
            List<Row> metadataRows,
            List<Row> detailRows,
            RahaDataset dataset,
            Set<String> includedColumns) {
        Map<String, Row> summaries = new LinkedHashMap<String, Row>();
        for (Row row : metadataRows) {
            if (CLUSTER_SUMMARY.equals(row.getAs("record_type"))) {
                String column = row.getAs("column_name");
                if (includedColumns.isEmpty()
                        || includedColumns.contains(column)) {
                    summaries.put(clusterKey(column,
                            (String) row.getAs("cluster_version")), row);
                }
            }
        }
        Map<String, List<ClusterAssignment>> members =
                new LinkedHashMap<String, List<ClusterAssignment>>();
        for (Row row : detailRows) {
            String column = row.getAs("column_name");
            String version = row.getAs("cluster_version");
            String key = clusterKey(column, version);
            if (!members.containsKey(key)) {
                members.put(key, new ArrayList<ClusterAssignment>());
            }
            CellCoordinate coordinate = new CellCoordinate(dataset.getDatasetId(),
                    dataset.getSnapshotId(), (String) row.getAs("row_id"), column);
            members.get(key).add(new ClusterAssignment(
                    (String) row.getAs("cell_id"), column, coordinate,
                    (String) row.getAs("cluster_id"), "RESTORED_CLUSTER",
                    version, null));
        }
        Map<String, ColumnClusteringResult> results =
                new LinkedHashMap<String, ColumnClusteringResult>();
        long assignmentCount = 0L;
        long clusteredColumns = 0L;
        for (Map.Entry<String, Row> entry : summaries.entrySet()) {
            List<ClusterAssignment> assignments = members.containsKey(entry.getKey())
                    ? members.get(entry.getKey())
                    : new ArrayList<ClusterAssignment>();
            Collections.sort(assignments,
                    Comparator.comparing(ClusterAssignment::getCellId));
            Row summary = entry.getValue();
            ColumnClusteringResult result = FmdbClusterSummaryCodec.read(
                    (String) summary.getAs("column_name"),
                    (String) summary.getAs("cluster_version"),
                    (String) summary.getAs("cluster_summary_json"), assignments);
            results.put(result.getColumnName(), result);
            assignmentCount += assignments.size();
            if (!assignments.isEmpty()) {
                clusteredColumns++;
            }
        }
        return new ClusteringBatchResult(results, new ClusteringMetrics(
                results.size(), clusteredColumns, assignmentCount,
                results.size() - clusteredColumns));
    }

    private static Map<String, List<SparseFeatureRow>> featuresByColumn(
            List<SparseFeatureRow> rows) {
        Map<String, List<SparseFeatureRow>> result =
                new LinkedHashMap<String, List<SparseFeatureRow>>();
        for (SparseFeatureRow row : rows) {
            if (!result.containsKey(row.getColumnName())) {
                result.put(row.getColumnName(), new ArrayList<SparseFeatureRow>());
            }
            result.get(row.getColumnName()).add(row);
        }
        return result;
    }

    private static Map<String, Map<String, ClusterAssignment>> assignmentsByColumn(
            ClusteringBatchResult clustering) {
        Map<String, Map<String, ClusterAssignment>> result =
                new LinkedHashMap<String, Map<String, ClusterAssignment>>();
        for (ColumnClusteringResult column : clustering.getResults().values()) {
            Map<String, ClusterAssignment> assignments =
                    new HashMap<String, ClusterAssignment>();
            for (ClusterAssignment assignment : column.getAssignments()) {
                assignments.put(assignment.getCellId(), assignment);
            }
            result.put(column.getColumnName(), assignments);
        }
        return result;
    }

    private PendingCheckpoint requiredPending(
            SnapshotCheckpointWriteSession session) {
        PendingCheckpoint pending = pendingCheckpoints.get(
                session.getCheckpointId());
        if (pending == null) {
            throw new IllegalStateException("检查点写会话不存在或已经完成："
                    + session.getCheckpointId());
        }
        return pending;
    }

    private void commitPath(String temporaryPath, String finalPath) {
        try {
            Path temporary = new Path(temporaryPath);
            Path target = new Path(finalPath);
            FileSystem fileSystem = temporary.getFileSystem(
                    sparkSession.sparkContext().hadoopConfiguration());
            if (fileSystem.exists(target)) {
                throw new IllegalStateException("检查点 HDFS 目标路径已存在：" + finalPath);
            }
            if (!fileSystem.rename(temporary, target)) {
                throw new IllegalStateException("检查点 HDFS 原子提交失败：" + finalPath);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("检查点 HDFS 原子提交异常：" + finalPath,
                    exception);
        }
    }

    private void deletePath(String path) {
        try {
            Path target = new Path(path);
            FileSystem fileSystem = target.getFileSystem(
                    sparkSession.sparkContext().hadoopConfiguration());
            if (fileSystem.exists(target) && !fileSystem.delete(target, true)) {
                throw new IllegalStateException("检查点 HDFS 路径清理失败：" + path);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("检查点 HDFS 路径清理异常：" + path,
                    exception);
        }
    }

    private void deletePathQuietly(String path) {
        try {
            deletePath(path);
        } catch (RuntimeException exception) {
            LOGGER.warn("检查点 HDFS 临时路径清理失败，path={}", path, exception);
        }
    }

    private String checkpointRoot(String checkpointId,
                                  DatasetSnapshot snapshot) {
        return trimTrailingSlash(
                persistenceConfig.getSnapshotCheckpointDetailBasePath())
                + "/dataset=" + HashUtils.md5Hex(snapshot.getDatasetId())
                + "/snapshot=" + HashUtils.md5Hex(snapshot.getSnapshotId())
                + "/checkpoint=" + checkpointId;
    }

    private String batchPath(String checkpointId,
                             DatasetSnapshot snapshot,
                             int batchIndex) {
        return checkpointRoot(checkpointId, snapshot) + "/column_batch="
                + String.format("%04d", Integer.valueOf(batchIndex));
    }

    private static String trimTrailingSlash(String path) {
        String result = ValueUtils.requireNotBlank(path, "检查点 HDFS 明细根目录");
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    private static Row latestActiveManifest(List<Row> rows) {
        Set<String> cleaned = new LinkedHashSet<String>();
        for (Row row : rows) {
            if (CLEANED.equals(row.getAs("record_type"))) {
                cleaned.add((String) row.getAs("checkpoint_id"));
            }
        }
        for (Row row : rows) {
            if (MANIFEST.equals(row.getAs("record_type"))
                    && !cleaned.contains((String) row.getAs("checkpoint_id"))) {
                return row;
            }
        }
        return null;
    }

    private static Row requiredManifest(List<Row> rows) {
        for (Row row : rows) {
            if (MANIFEST.equals(row.getAs("record_type"))) {
                return row;
            }
        }
        throw new IllegalStateException("快照检查点缺少最终清单");
    }

    private static FmdbTableRecord cleanedRecord(Row manifest,
                                                  long cleanedAt,
                                                  String detailRootPath) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        for (String column : FmdbTableSchemas.columns(
                FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)) {
            values.put(column, manifest.getAs(column));
        }
        values.put("record_type", CLEANED);
        values.put("record_scope", SCOPE_CHECKPOINT);
        values.put("payload_json", FmdbJsonCodec.write(Collections.singletonMap(
                "detailRootPath", detailRootPath)));
        values.put("created_at", Long.valueOf(cleanedAt));
        values.put("partition_month", FmdbPartitionUtils.month(cleanedAt));
        return FmdbTableRecord.of(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT, values);
    }

    private static Map<String, Object> detailPayload(
            DetailBatch detail,
            List<String> columns) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("schemaVersion", DETAIL_SCHEMA_VERSION);
        payload.put("batchIndex", Integer.valueOf(detail.batchIndex));
        payload.put("path", detail.path);
        payload.put("columns", new ArrayList<String>(columns));
        payload.put("recordCount", Long.valueOf(detail.recordCount));
        payload.put("fingerprint", detail.fingerprint);
        return payload;
    }

    private static Map<String, Object> manifestPayload(
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            String strategyPlanVersion,
            List<Map<String, Object>> batches,
            String detailRootPath) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("schemaVersion", DETAIL_SCHEMA_VERSION);
        payload.put("datasetId", snapshot.getDatasetId());
        payload.put("snapshotId", snapshot.getSnapshotId());
        payload.put("inputReference", snapshot.getInputReference());
        payload.put("tableName", snapshot.getTableName());
        payload.put("rowIdColumn", snapshot.getRowIdColumn());
        payload.put("schemaHash", snapshot.getSchemaHash());
        payload.put("rowCount", Long.valueOf(snapshot.getRowCount()));
        payload.put("columnCount", Integer.valueOf(snapshot.getColumnCount()));
        payload.put("createdAt", Long.valueOf(snapshot.getCreatedAt()));
        payload.put("strategyPlanVersion", strategyPlanVersion);
        payload.put("detailRootPath", detailRootPath);
        payload.put("detailBatches", batches);
        List<Map<String, Object>> columns =
                new ArrayList<Map<String, Object>>();
        for (ColumnMetadata column : dataset.getColumns()) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("name", column.getName());
            item.put("ordinal", Integer.valueOf(column.getOrdinal()));
            item.put("dataType", column.getDataType());
            item.put("nullable", Boolean.valueOf(column.isNullable()));
            item.put("detectable", Boolean.valueOf(column.isDetectable()));
            item.put("sensitive", Boolean.valueOf(column.isSensitive()));
            columns.add(item);
        }
        payload.put("columns", columns);
        return FmdbJsonCodec.readObject(FmdbJsonCodec.write(payload));
    }

    private static DatasetSnapshot snapshot(Map<String, Object> payload,
                                            Row row) {
        return new DatasetSnapshot(
                FmdbJsonValue.requiredText(payload, "datasetId"),
                FmdbJsonValue.requiredText(payload, "snapshotId"),
                FmdbJsonValue.requiredText(payload, "inputReference"),
                FmdbJsonValue.requiredText(payload, "tableName"),
                FmdbJsonValue.requiredText(payload, "rowIdColumn"),
                (String) row.getAs("schema_hash"),
                FmdbJsonValue.requiredNumber(payload, "rowCount").longValue(),
                FmdbJsonValue.requiredNumber(payload, "columnCount").intValue(),
                (String) row.getAs("source_version"),
                FmdbJsonValue.requiredNumber(payload, "createdAt").longValue());
    }

    private static List<ColumnMetadata> columns(Map<String, Object> payload) {
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        for (Object raw : FmdbJsonValue.objectList(payload, "columns")) {
            Map<String, Object> item = FmdbJsonValue.objectMap(raw, "columns");
            columns.add(new ColumnMetadata(
                    FmdbJsonValue.requiredText(item, "name"),
                    FmdbJsonValue.requiredNumber(item, "ordinal").intValue(),
                    FmdbJsonValue.requiredText(item, "dataType"),
                    booleanValue(item, "nullable"),
                    booleanValue(item, "detectable"),
                    booleanValue(item, "sensitive")));
        }
        Collections.sort(columns,
                Comparator.comparingInt(ColumnMetadata::getOrdinal));
        return columns;
    }

    private static List<ColumnMetadata> selectedColumns(
            List<ColumnMetadata> available,
            Set<String> includedColumns) {
        if (includedColumns.isEmpty()) {
            return available;
        }
        List<ColumnMetadata> selected = new ArrayList<ColumnMetadata>();
        for (ColumnMetadata column : available) {
            if (!column.isDetectable()
                    || includedColumns.contains(column.getName())) {
                selected.add(column);
            }
        }
        return selected;
    }

    private static void validateRequestedColumns(
            List<ColumnMetadata> available,
            Set<String> requested) {
        if (requested.isEmpty()) {
            return;
        }
        Set<String> detectable = new LinkedHashSet<String>();
        for (ColumnMetadata column : available) {
            if (column.isDetectable()) {
                detectable.add(column.getName());
            }
        }
        if (!detectable.containsAll(requested)) {
            Set<String> missing = new LinkedHashSet<String>(requested);
            missing.removeAll(detectable);
            throw new IllegalArgumentException("检查点不包含请求字段：" + missing);
        }
    }

    private static Set<String> normalizedColumns(Set<String> source) {
        if (source == null || source.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new LinkedHashSet<String>();
        for (String column : source) {
            result.add(ValueUtils.requireNotBlank(column, "检查点恢复字段"));
        }
        return Collections.unmodifiableSet(result);
    }

    private static Map<String, Object> base(String checkpointId,
                                            String sourceJobId,
                                            DatasetSnapshot snapshot,
                                            String rowSetFingerprint,
                                            String configFingerprint,
                                            long createdAt) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("checkpoint_id", checkpointId);
        values.put("dataset_id", snapshot.getDatasetId());
        values.put("snapshot_id", snapshot.getSnapshotId());
        values.put("source_job_id", sourceJobId);
        values.put("record_type", null);
        values.put("record_scope", null);
        values.put("column_name", null);
        values.put("artifact_version", null);
        values.put("profile_json", null);
        values.put("strategy_plan_json", null);
        values.put("feature_dictionary_json", null);
        values.put("cluster_version", null);
        values.put("cluster_summary_json", null);
        values.put("payload_json", null);
        values.put("row_set_fingerprint", rowSetFingerprint);
        values.put("config_fingerprint", configFingerprint);
        values.put("schema_hash", snapshot.getSchemaHash());
        values.put("source_version", snapshot.getSourceVersion());
        values.put("created_at", Long.valueOf(createdAt));
        values.put("partition_month", FmdbPartitionUtils.month(createdAt));
        return values;
    }

    private static FmdbTableRecord record(Map<String, Object> values,
                                          String recordType,
                                          String recordScope,
                                          String columnName,
                                          Map<String, Object> payload) {
        values.put("record_type", recordType);
        values.put("record_scope", recordScope);
        if (columnName != null) {
            values.put("column_name", columnName);
        }
        if (payload != null) {
            values.put("payload_json", FmdbJsonCodec.write(payload));
        }
        return FmdbTableRecord.of(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT, values);
    }

    private static List<StrategyRunSummary> summaries(
            StrategyBatchResult strategyBatch) {
        List<StrategyRunSummary> summaries = new ArrayList<StrategyRunSummary>();
        for (StrategyExecutionResult execution : strategyBatch.getExecutions()) {
            summaries.add(execution.getSummary());
        }
        return summaries;
    }

    private static Map<Integer, Double> integerDoubleMap(Map<?, ?> source) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            values.put(((Number) entry.getKey()).intValue(),
                    ((Number) entry.getValue()).doubleValue());
        }
        return values;
    }

    private static List<String> stringList(Map<String, Object> values,
                                           String key) {
        Object raw = values.get(key);
        if (!(raw instanceof List)) {
            throw new IllegalArgumentException("检查点批次清单缺少字段：" + key);
        }
        List<String> result = new ArrayList<String>();
        for (Object item : (List<?>) raw) {
            result.add(String.valueOf(item));
        }
        return result;
    }

    private static boolean intersects(List<String> columns,
                                      Set<String> requested) {
        for (String column : columns) {
            if (requested.contains(column)) {
                return true;
            }
        }
        return false;
    }

    private static boolean booleanValue(Map<String, Object> values,
                                        String key) {
        Object value = values.get(key);
        if (!(value instanceof Boolean)) {
            throw new IllegalArgumentException("检查点列元数据缺少布尔字段：" + key);
        }
        return ((Boolean) value).booleanValue();
    }

    private static String clusterKey(String columnName, String clusterVersion) {
        return columnName + "|" + clusterVersion;
    }

    private static MessageDigest messageDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("运行环境缺少 SHA-256 摘要算法", exception);
        }
    }

    private static void updateChecksum(MessageDigest digest,
                                       SparseFeatureRow feature,
                                       ClusterAssignment assignment) {
        update(digest, feature.getColumnName());
        update(digest, assignment.getCoordinate().getRowId());
        update(digest, feature.getCellId());
        update(digest, feature.getValueHash());
        update(digest, feature.getFeatureDictionaryVersion());
        List<Integer> indexes = new ArrayList<Integer>(
                feature.getValues().keySet());
        Collections.sort(indexes);
        for (Integer index : indexes) {
            update(digest, String.valueOf(index));
            update(digest, String.valueOf(feature.getValues().get(index)));
        }
        update(digest, assignment.getClusterVersion());
        update(digest, assignment.getClusterId());
    }

    private static void update(MessageDigest digest, String value) {
        digest.update(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
    }

    private static String hex(byte[] values) {
        StringBuilder result = new StringBuilder(values.length * 2);
        for (byte value : values) {
            result.append(String.format("%02x", Integer.valueOf(value & 0xff)));
        }
        return result.toString();
    }

    private static String checkpointId(String sourceJobId,
                                       DatasetSnapshot snapshot,
                                       String configFingerprint,
                                       long createdAt) {
        return "snapshot_" + HashUtils.md5Hex(snapshot.getDatasetId() + "|"
                + snapshot.getSnapshotId() + "|" + configFingerprint + "|"
                + sourceJobId + "|" + createdAt);
    }

    private static String rowSetFingerprint(DatasetSnapshot snapshot) {
        return HashUtils.md5Hex(snapshot.getDatasetId() + "|"
                + snapshot.getSnapshotId() + "|" + snapshot.getSchemaHash()
                + "|" + snapshot.getRowCount() + "|" + snapshot.getColumnCount());
    }

    /** 尚未提交最终清单的检查点写状态。 */
    private static final class PendingCheckpoint {

        /** 首个列批需要一并提交的全局元数据。 */
        private final List<FmdbTableRecord> globalRecords;
        /** 输入行集合指纹。 */
        private final String rowSetFingerprint;
        /** 已成功提交的列批清单。 */
        private final List<Map<String, Object>> batchPayloads =
                new ArrayList<Map<String, Object>>();
        /** 已成功提交的字段集合。 */
        private final Set<String> completedColumns =
                new LinkedHashSet<String>();
        /** 下一个允许提交的列批序号。 */
        private int nextBatchIndex = 1;
        /** 已成功提交的 HDFS 明细总数。 */
        private long detailRecordCount;

        private PendingCheckpoint(List<FmdbTableRecord> globalRecords,
                                  String rowSetFingerprint) {
            this.globalRecords = new ArrayList<FmdbTableRecord>(globalRecords);
            this.rowSetFingerprint = rowSetFingerprint;
        }
    }

    /** HDFS 明细列批提交结果。 */
    private static final class DetailBatch {

        /** 列批序号。 */
        private final int batchIndex;
        /** 已原子提交的 HDFS 路径。 */
        private final String path;
        /** 列批单元格记录数。 */
        private final long recordCount;
        /** 列批内容指纹。 */
        private final String fingerprint;

        private DetailBatch(int batchIndex,
                            String path,
                            long recordCount,
                            String fingerprint) {
            this.batchIndex = batchIndex;
            this.path = path;
            this.recordCount = recordCount;
            this.fingerprint = fingerprint;
        }
    }

    /** 恢复前需要校验的 HDFS 明细引用。 */
    private static final class DetailReference {

        /** ORC 列批路径。 */
        private final String path;
        /** 清单声明的记录数。 */
        private final long recordCount;

        private DetailReference(String path, long recordCount) {
            this.path = path;
            this.recordCount = recordCount;
        }

        @Override
        public String toString() {
            return path + "#" + recordCount;
        }
    }

    /** 策略计划和执行摘要恢复结果。 */
    private static final class StrategyArtifacts {

        /** 策略计划。 */
        private final List<StrategyPlan> plans;
        /** 策略执行摘要。 */
        private final StrategyBatchResult batch;

        private StrategyArtifacts(List<StrategyPlan> plans,
                                  StrategyBatchResult batch) {
            this.plans = plans;
            this.batch = batch;
        }
    }
}
