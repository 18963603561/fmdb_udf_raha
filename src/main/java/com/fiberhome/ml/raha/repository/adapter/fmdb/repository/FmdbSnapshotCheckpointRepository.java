package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用统一 FMDB 检查点表保存和恢复采样阶段前置产物。
 */
public final class FmdbSnapshotCheckpointRepository
        implements SnapshotCheckpointRepository {

    /** manifest 行类型。 */
    private static final String MANIFEST = "MANIFEST";
    /** 列画像行类型。 */
    private static final String PROFILE = "PROFILE";
    /** 策略计划行类型。 */
    private static final String STRATEGY_PLAN = "STRATEGY_PLAN";
    /** 特征字典行类型。 */
    private static final String FEATURE_DICTIONARY = "FEATURE_DICTIONARY";
    /** 单元格特征行类型。 */
    private static final String CELL_FEATURE = "CELL_FEATURE";
    /** 聚类摘要行类型。 */
    private static final String CLUSTER_SUMMARY = "CLUSTER_SUMMARY";
    /** 聚类成员行类型。 */
    private static final String CLUSTER_ASSIGNMENT = "CLUSTER_ASSIGNMENT";
    /** 检查点级记录作用域。 */
    private static final String SCOPE_CHECKPOINT = "CHECKPOINT";
    /** 列级记录作用域。 */
    private static final String SCOPE_COLUMN = "COLUMN";
    /** 单元格级记录作用域。 */
    private static final String SCOPE_CELL = "CELL";

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
    public void save(String sourceJobId,
                     RahaDataset dataset,
                     DatasetSnapshot snapshot,
                     List<StrategyPlan> strategyPlans,
                     String strategyPlanVersion,
                     StrategyBatchResult strategyBatch,
                     FeatureAssemblyResult features,
                     ClusteringBatchResult clustering,
                     String configFingerprint,
                     long createdAt) {
        String jobId = ValueUtils.requireNotBlank(sourceJobId, "来源任务标识");
        if (dataset == null || snapshot == null || strategyPlans == null
                || strategyBatch == null || features == null || clustering == null
                || createdAt <= 0L) {
            throw new IllegalArgumentException("快照检查点保存参数不能为空");
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)) {
            LOGGER.info("FMDB 快照检查点持久化已关闭，跳过写入，jobId={}，snapshotId={}",
                    jobId, snapshot.getSnapshotId());
            return;
        }
        String fingerprint = ValueUtils.requireNotBlank(configFingerprint,
                "执行配置指纹");
        String checkpointId = checkpointId(jobId, snapshot, fingerprint);
        String rowSetFingerprint = rowSetFingerprint(snapshot);
        List<FmdbTableRecord> records = new ArrayList<FmdbTableRecord>();
        records.add(record(base(checkpointId, jobId, snapshot, rowSetFingerprint,
                fingerprint, createdAt), MANIFEST, SCOPE_CHECKPOINT, null,
                manifestPayload(dataset, snapshot, strategyPlanVersion)));

        List<ColumnProfile> profiles = new ArrayList<ColumnProfile>(
                dataset.getProfiles().values());
        Collections.sort(profiles, Comparator.comparing(ColumnProfile::getColumnName));
        for (ColumnProfile profile : profiles) {
            Map<String, Object> values = base(checkpointId, jobId, snapshot,
                    rowSetFingerprint, fingerprint, createdAt);
            values.put("column_name", profile.getColumnName());
            values.put("artifact_version", snapshot.getSnapshotId());
            values.put("profile_json", FmdbColumnProfileCodec.write(profile));
            records.add(record(values, PROFILE, SCOPE_COLUMN, profile.getColumnName(),
                    null));
        }

        Map<String, Object> strategyValues = base(checkpointId, jobId, snapshot,
                rowSetFingerprint, fingerprint, createdAt);
        strategyValues.put("artifact_version", strategyPlanVersion);
        strategyValues.put("strategy_plan_json", FmdbStrategyArtifactCodec.write(
                strategyPlans, summaries(strategyBatch)));
        records.add(record(strategyValues, STRATEGY_PLAN, SCOPE_CHECKPOINT,
                null, null));

        List<String> dictionaryColumns = new ArrayList<String>(
                features.getDictionaries().keySet());
        Collections.sort(dictionaryColumns);
        for (String column : dictionaryColumns) {
            FeatureDictionary dictionary = features.getDictionaries().get(column);
            Map<String, Object> values = base(checkpointId, jobId, snapshot,
                    rowSetFingerprint, fingerprint, createdAt);
            values.put("column_name", column);
            values.put("artifact_version", dictionary.getVersion());
            values.put("feature_dictionary_json",
                    FmdbFeatureDictionaryCodec.write(dictionary));
            records.add(record(values, FEATURE_DICTIONARY, SCOPE_COLUMN,
                    column, null));
        }

        List<SparseFeatureRow> featureRows = new ArrayList<SparseFeatureRow>(
                features.getRows());
        Collections.sort(featureRows, Comparator.comparing(
                SparseFeatureRow::getCellId));
        for (SparseFeatureRow row : featureRows) {
            Map<String, Object> values = base(checkpointId, jobId, snapshot,
                    rowSetFingerprint, fingerprint, createdAt);
            values.put("column_name", row.getColumnName());
            values.put("row_id", row.getCoordinate() == null ? null
                    : row.getCoordinate().getRowId());
            values.put("cell_id", row.getCellId());
            values.put("cell_value", row.getMaskedValue());
            values.put("artifact_version", row.getFeatureDictionaryVersion());
            values.put("feature_vector_json", featureValues(row.getValues()));
            values.put("feature_summary_json", FmdbJsonCodec.write(row.getSummary()));
            records.add(record(values, CELL_FEATURE, SCOPE_CELL,
                    row.getColumnName(), null));
        }

        List<ColumnClusteringResult> clusteringResults =
                new ArrayList<ColumnClusteringResult>(
                        clustering.getResults().values());
        Collections.sort(clusteringResults, Comparator.comparing(
                ColumnClusteringResult::getColumnName));
        for (ColumnClusteringResult result : clusteringResults) {
            Map<String, Object> values = base(checkpointId, jobId, snapshot,
                    rowSetFingerprint, fingerprint, createdAt);
            values.put("column_name", result.getColumnName());
            values.put("artifact_version", result.getClusterVersion());
            values.put("cluster_version", result.getClusterVersion());
            values.put("cluster_summary_json",
                    FmdbClusterSummaryCodec.write(result));
            records.add(record(values, CLUSTER_SUMMARY, SCOPE_COLUMN,
                    result.getColumnName(), null));
            for (ClusterAssignment assignment : result.getAssignments()) {
                Map<String, Object> item = base(checkpointId, jobId, snapshot,
                        rowSetFingerprint, fingerprint, createdAt);
                item.put("column_name", assignment.getColumnName());
                item.put("row_id", assignment.getCoordinate() == null ? null
                        : assignment.getCoordinate().getRowId());
                item.put("cell_id", assignment.getCellId());
                item.put("artifact_version", assignment.getClusterVersion());
                item.put("cluster_version", assignment.getClusterVersion());
                item.put("cluster_id", assignment.getClusterId());
                item.put("cluster_distance", assignment.getDistance());
                records.add(record(item, CLUSTER_ASSIGNMENT, SCOPE_CELL,
                        assignment.getColumnName(), null));
            }
        }
        append(records);
        LOGGER.info("FMDB 快照检查点写入完成，checkpointId={}，datasetId={}，"
                        + "snapshotId={}，recordCount={}",
                checkpointId, snapshot.getDatasetId(), snapshot.getSnapshotId(),
                records.size());
    }

    @Override
    public Optional<SnapshotPreparedArtifacts> restore(String datasetId,
                                                       String snapshotId,
                                                       String configFingerprint) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        String fingerprint = ValueUtils.requireNotBlank(configFingerprint,
                "执行配置指纹");
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT)
                || !tableGateway.tableExists(tableName)) {
            return Optional.empty();
        }
        List<Row> manifests = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT),
                functions.col("dataset_id").equalTo(dataset)
                        .and(functions.col("snapshot_id").equalTo(snapshot))
                        .and(functions.col("config_fingerprint").equalTo(fingerprint))
                        .and(functions.col("record_type").equalTo(MANIFEST)))
                .orderBy(functions.col("created_at").desc()).limit(1)
                .collectAsList();
        if (manifests.isEmpty()) {
            LOGGER.warn("未找到可复用快照检查点，datasetId={}，snapshotId={}",
                    dataset, snapshot);
            return Optional.empty();
        }
        String checkpointId = manifests.get(0).getAs("checkpoint_id");
        List<Row> rows = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT),
                functions.col("checkpoint_id").equalTo(checkpointId))
                .collectAsList();
        SnapshotPreparedArtifacts artifacts = restoreArtifacts(checkpointId,
                fingerprint, rows);
        LOGGER.info("FMDB 快照检查点恢复完成，checkpointId={}，datasetId={}，"
                        + "snapshotId={}，featureRowCount={}，assignmentCount={}",
                checkpointId, artifacts.getDataset().getDatasetId(),
                artifacts.getDataset().getSnapshotId(),
                artifacts.getFeatures().getRows().size(),
                artifacts.getClustering().getMetrics().getAssignmentCount());
        return Optional.of(artifacts);
    }

    private SnapshotPreparedArtifacts restoreArtifacts(String checkpointId,
                                                       String configFingerprint,
                                                       List<Row> rows) {
        Row manifest = manifest(rows);
        Map<String, Object> payload = FmdbJsonCodec.readObject(
                (String) manifest.getAs("payload_json"));
        DatasetSnapshot snapshot = snapshot(payload, manifest);
        List<ColumnMetadata> columns = columns(payload);
        Map<String, ColumnProfile> profiles = profiles(rows);
        RahaDataset dataset = new RahaDataset(snapshot.getDatasetId(),
                snapshot.getSnapshotId(), snapshot.getTableName(),
                snapshot.getRowIdColumn(), columns, null,
                snapshot.getSchemaHash(), profiles);
        StrategyArtifacts strategy = strategyArtifacts(rows);
        FeatureAssemblyResult features = features(rows);
        ClusteringBatchResult clustering = clustering(rows, dataset);
        return new SnapshotPreparedArtifacts(checkpointId, dataset, snapshot,
                strategy.plans, strategy.batch, features,
                FmdbJsonValue.requiredText(payload, "strategyPlanVersion"),
                clustering, configFingerprint);
    }

    private void append(List<FmdbTableRecord> records) {
        List<Row> rows = new ArrayList<Row>(records.size());
        for (FmdbTableRecord record : records) {
            rows.add(record.toRow());
        }
        Dataset<Row> frame = sparkSession.createDataFrame(rows,
                FmdbTableSchemas.schema(FmdbPhysicalTable.SNAPSHOT_CHECKPOINT));
        tableGateway.appendDirect(tableName, frame, records.size());
    }

    private static Row manifest(List<Row> rows) {
        for (Row row : rows) {
            if (MANIFEST.equals(row.getAs("record_type"))) {
                return row;
            }
        }
        throw new IllegalStateException("快照检查点缺少 manifest 记录");
    }

    private static Map<String, ColumnProfile> profiles(List<Row> rows) {
        Map<String, ColumnProfile> profiles =
                new LinkedHashMap<String, ColumnProfile>();
        for (Row row : rows) {
            if (PROFILE.equals(row.getAs("record_type"))) {
                ColumnProfile profile = FmdbColumnProfileCodec.read(
                        (String) row.getAs("profile_json"));
                profiles.put(profile.getColumnName(), profile);
            }
        }
        return profiles;
    }

    private static StrategyArtifacts strategyArtifacts(List<Row> rows) {
        Map<String, StrategyPlan> plans = new LinkedHashMap<String, StrategyPlan>();
        Map<String, StrategyRunSummary> summaries =
                new LinkedHashMap<String, StrategyRunSummary>();
        for (Row row : rows) {
            if (STRATEGY_PLAN.equals(row.getAs("record_type"))) {
                String json = (String) row.getAs("strategy_plan_json");
                for (StrategyPlan plan : FmdbStrategyArtifactCodec.readPlans(json)) {
                    plans.put(plan.getStrategyId(), plan);
                }
                for (StrategyRunSummary summary
                        : FmdbStrategyArtifactCodec.readSummaries(json)) {
                    summaries.put(summary.getStrategyId(), summary);
                }
            }
        }
        List<StrategyExecutionResult> executions =
                new ArrayList<StrategyExecutionResult>(summaries.size());
        for (StrategyRunSummary summary : summaries.values()) {
            executions.add(StrategyExecutionResult.summaryOnly(summary));
        }
        return new StrategyArtifacts(new ArrayList<StrategyPlan>(plans.values()),
                new StrategyBatchResult(executions));
    }

    private static FeatureAssemblyResult features(List<Row> rows) {
        Map<String, FeatureDictionary> dictionaries =
                new LinkedHashMap<String, FeatureDictionary>();
        Map<String, SparseFeatureRow> featureRows =
                new LinkedHashMap<String, SparseFeatureRow>();
        for (Row row : rows) {
            String recordType = row.getAs("record_type");
            if (FEATURE_DICTIONARY.equals(recordType)) {
                FeatureDictionary dictionary = FmdbFeatureDictionaryCodec.read(
                        (String) row.getAs("feature_dictionary_json"));
                dictionaries.put(dictionary.getColumnName(), dictionary);
            } else if (CELL_FEATURE.equals(recordType)) {
                SparseFeatureRow featureRow = new SparseFeatureRow(
                        (String) row.getAs("cell_id"),
                        (String) row.getAs("column_name"),
                        (String) row.getAs("artifact_version"),
                        featureValues((String) row.getAs("feature_vector_json")),
                        stringValues((String) row.getAs("feature_summary_json")));
                featureRows.put(featureRow.getCellId(), featureRow);
            }
        }
        long retained = 0L;
        for (FeatureDictionary dictionary : dictionaries.values()) {
            retained += dictionary.getDefinitions().size();
        }
        List<SparseFeatureRow> orderedRows =
                new ArrayList<SparseFeatureRow>(featureRows.values());
        Collections.sort(orderedRows, Comparator.comparing(
                SparseFeatureRow::getCellId));
        return new FeatureAssemblyResult(dictionaries, orderedRows,
                new FeatureAssemblyMetrics(orderedRows.size(), retained,
                        retained, 0L));
    }

    private static ClusteringBatchResult clustering(List<Row> rows,
                                                    RahaDataset dataset) {
        Map<String, Row> summaries = new LinkedHashMap<String, Row>();
        Map<String, Map<String, ClusterAssignment>> assignmentGroups =
                new LinkedHashMap<String, Map<String, ClusterAssignment>>();
        for (Row row : rows) {
            String recordType = row.getAs("record_type");
            if (CLUSTER_SUMMARY.equals(recordType)) {
                summaries.put(clusterKey(row), row);
            } else if (CLUSTER_ASSIGNMENT.equals(recordType)) {
                String key = clusterKey(row);
                if (!assignmentGroups.containsKey(key)) {
                    assignmentGroups.put(key,
                            new LinkedHashMap<String, ClusterAssignment>());
                }
                ClusterAssignment assignment = assignment(row, dataset);
                assignmentGroups.get(key).put(assignment.getCellId(), assignment);
            }
        }
        Map<String, ColumnClusteringResult> results =
                new LinkedHashMap<String, ColumnClusteringResult>();
        long assignments = 0L;
        long clusteredColumns = 0L;
        for (Map.Entry<String, Row> entry : summaries.entrySet()) {
            Row summary = entry.getValue();
            List<ClusterAssignment> members = new ArrayList<ClusterAssignment>(
                    assignmentGroups.containsKey(entry.getKey())
                            ? assignmentGroups.get(entry.getKey()).values()
                            : Collections.<ClusterAssignment>emptyList());
            Collections.sort(members, Comparator.comparing(
                    ClusterAssignment::getCellId));
            ColumnClusteringResult result = FmdbClusterSummaryCodec.read(
                    (String) summary.getAs("column_name"),
                    (String) summary.getAs("cluster_version"),
                    (String) summary.getAs("cluster_summary_json"), members);
            results.put(result.getColumnName(), result);
            assignments += members.size();
            if (!members.isEmpty()) {
                clusteredColumns++;
            }
        }
        return new ClusteringBatchResult(results, new ClusteringMetrics(
                results.size(), clusteredColumns, assignments,
                results.size() - clusteredColumns));
    }

    private static ClusterAssignment assignment(Row row, RahaDataset dataset) {
        String rowId = row.getAs("row_id");
        String columnName = row.getAs("column_name");
        CellCoordinate coordinate = rowId == null ? null
                : new CellCoordinate(dataset.getDatasetId(),
                dataset.getSnapshotId(), rowId, columnName);
        Object distance = row.getAs("cluster_distance");
        return new ClusterAssignment((String) row.getAs("cell_id"), columnName,
                coordinate, (String) row.getAs("cluster_id"),
                algorithm(row), (String) row.getAs("cluster_version"),
                distance == null ? null : ((Number) distance).doubleValue());
    }

    private static String algorithm(Row row) {
        return "RESTORED_CLUSTER";
    }

    private static String clusterKey(Row row) {
        return String.valueOf(row.getAs("column_name")) + "|"
                + String.valueOf(row.getAs("cluster_version"));
    }

    private static DatasetSnapshot snapshot(Map<String, Object> payload, Row row) {
        return new DatasetSnapshot(
                FmdbJsonValue.requiredText(payload, "datasetId"),
                FmdbJsonValue.requiredText(payload, "snapshotId"),
                FmdbJsonValue.requiredText(payload, "inputReference"),
                FmdbJsonValue.requiredText(payload, "tableName"),
                FmdbJsonValue.requiredText(payload, "rowIdColumn"),
                (String) row.getAs("schema_hash"),
                FmdbJsonValue.requiredNumber(payload, "rowCount").longValue(),
                FmdbJsonValue.requiredNumber(payload, "columnCount").intValue(),
                row.getAs("source_version"),
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
        Collections.sort(columns, Comparator.comparingInt(
                ColumnMetadata::getOrdinal));
        return columns;
    }

    private static Boolean booleanValue(Map<String, Object> values, String key) {
        Object value = values.get(key);
        if (!(value instanceof Boolean)) {
            throw new IllegalArgumentException("检查点列元数据缺少布尔字段：" + key);
        }
        return (Boolean) value;
    }

    private static Map<String, Object> manifestPayload(
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            String strategyPlanVersion) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("datasetId", snapshot.getDatasetId());
        payload.put("snapshotId", snapshot.getSnapshotId());
        payload.put("inputReference", snapshot.getInputReference());
        payload.put("tableName", snapshot.getTableName());
        payload.put("rowIdColumn", snapshot.getRowIdColumn());
        payload.put("schemaHash", snapshot.getSchemaHash());
        payload.put("rowCount", snapshot.getRowCount());
        payload.put("columnCount", snapshot.getColumnCount());
        payload.put("createdAt", snapshot.getCreatedAt());
        payload.put("strategyPlanVersion", strategyPlanVersion);
        List<Map<String, Object>> columns =
                new ArrayList<Map<String, Object>>();
        for (ColumnMetadata column : dataset.getColumns()) {
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("name", column.getName());
            item.put("ordinal", column.getOrdinal());
            item.put("dataType", column.getDataType());
            item.put("nullable", column.isNullable());
            item.put("detectable", column.isDetectable());
            item.put("sensitive", column.isSensitive());
            columns.add(item);
        }
        payload.put("columns", columns);
        return FmdbJsonCodec.readObject(FmdbJsonCodec.write(payload));
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
        values.put("sample_batch_id", null);
        values.put("record_type", null);
        values.put("record_scope", null);
        values.put("column_name", null);
        values.put("row_id", null);
        values.put("cell_id", null);
        values.put("cell_value", null);
        values.put("artifact_version", null);
        values.put("profile_json", null);
        values.put("strategy_plan_json", null);
        values.put("strategy_hit_json", null);
        values.put("feature_dictionary_json", null);
        values.put("feature_vector_json", null);
        values.put("feature_summary_json", null);
        values.put("cluster_version", null);
        values.put("cluster_id", null);
        values.put("cluster_distance", null);
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

    private static String featureValues(Map<Integer, Double> values) {
        Map<String, Object> encoded = new LinkedHashMap<String, Object>();
        for (Map.Entry<Integer, Double> entry : values.entrySet()) {
            encoded.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        return FmdbJsonCodec.write(encoded);
    }

    private static Map<Integer, Double> featureValues(String json) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        for (Map.Entry<String, Object> entry : FmdbJsonCodec.readObject(json).entrySet()) {
            if (!(entry.getValue() instanceof Number)) {
                throw new IllegalArgumentException("检查点特征向量包含非数值字段");
            }
            values.put(Integer.valueOf(entry.getKey()),
                    ((Number) entry.getValue()).doubleValue());
        }
        return values;
    }

    private static Map<String, String> stringValues(String json) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Object> entry : FmdbJsonCodec.readObject(json).entrySet()) {
            if (!(entry.getValue() instanceof String)) {
                throw new IllegalArgumentException("检查点特征摘要包含非文本字段");
            }
            values.put(entry.getKey(), (String) entry.getValue());
        }
        return values;
    }

    private static String checkpointId(String sourceJobId,
                                       DatasetSnapshot snapshot,
                                       String configFingerprint) {
        return "snapshot_" + HashUtils.md5Hex(snapshot.getDatasetId() + "|"
                + snapshot.getSnapshotId() + "|" + configFingerprint + "|"
                + sourceJobId);
    }

    private static String rowSetFingerprint(DatasetSnapshot snapshot) {
        return HashUtils.md5Hex(snapshot.getDatasetId() + "|"
                + snapshot.getSnapshotId() + "|" + snapshot.getSchemaHash()
                + "|" + snapshot.getRowCount() + "|" + snapshot.getColumnCount());
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
