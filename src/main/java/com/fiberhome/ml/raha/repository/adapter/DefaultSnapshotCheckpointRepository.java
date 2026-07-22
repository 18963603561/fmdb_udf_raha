package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.checkpoint.SnapshotPreparedArtifacts;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringMetrics;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 基于进程内映射保存采样快照检查点。
 */
public final class DefaultSnapshotCheckpointRepository
        implements SnapshotCheckpointRepository {

    /** 按数据集、快照和配置指纹索引的最新检查点。 */
    private final Map<String, SnapshotPreparedArtifacts> checkpoints =
            new LinkedHashMap<String, SnapshotPreparedArtifacts>();

    @Override
    public synchronized void save(String sourceJobId,
                                  RahaDataset dataset,
                                  DatasetSnapshot snapshot,
                                  List<StrategyPlan> strategyPlans,
                                  String strategyPlanVersion,
                                  StrategyBatchResult strategyBatch,
                                  FeatureAssemblyResult features,
                                  ClusteringBatchResult clustering,
                                  String configFingerprint,
                                  long createdAt) {
        String key = key(dataset.getDatasetId(), snapshot.getSnapshotId(),
                configFingerprint);
        String checkpointId = "snapshot_"
                + HashUtils.md5Hex(key + "|" + sourceJobId + "|" + createdAt);
        checkpoints.put(key, new SnapshotPreparedArtifacts(checkpointId,
                dataset, snapshot, strategyPlans, strategyBatch, features,
                strategyPlanVersion, clustering, configFingerprint));
    }

    @Override
    public synchronized Optional<SnapshotPreparedArtifacts> restore(
            String datasetId,
            String snapshotId,
            String configFingerprint,
            Set<String> includedColumns) {
        SnapshotPreparedArtifacts artifacts = checkpoints.get(key(datasetId,
                snapshotId, configFingerprint));
        if (artifacts == null) {
            return Optional.empty();
        }
        Set<String> requestedColumns = normalizedColumns(includedColumns);
        return requestedColumns.isEmpty()
                ? Optional.of(artifacts)
                : Optional.of(filter(artifacts, requestedColumns));
    }

    private static SnapshotPreparedArtifacts filter(
            SnapshotPreparedArtifacts source,
            Set<String> includedColumns) {
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        Map<String, ColumnProfile> profiles =
                new LinkedHashMap<String, ColumnProfile>();
        Set<String> available = new LinkedHashSet<String>();
        for (ColumnMetadata column : source.getDataset().getColumns()) {
            if (column.isDetectable()) {
                available.add(column.getName());
            }
            if (!column.isDetectable()
                    || includedColumns.contains(column.getName())) {
                columns.add(column);
            }
        }
        if (!available.containsAll(includedColumns)) {
            Set<String> missing = new LinkedHashSet<String>(includedColumns);
            missing.removeAll(available);
            throw new IllegalArgumentException("检查点不包含请求的可检测字段：" + missing);
        }
        for (Map.Entry<String, ColumnProfile> entry
                : source.getDataset().getProfiles().entrySet()) {
            if (includedColumns.contains(entry.getKey())) {
                profiles.put(entry.getKey(), entry.getValue());
            }
        }
        RahaDataset dataset = new RahaDataset(source.getDataset().getDatasetId(),
                source.getDataset().getSnapshotId(),
                source.getDataset().getTableName(),
                source.getDataset().getRowIdColumn(), columns, null,
                source.getDataset().getSchemaHash(), profiles);

        List<StrategyPlan> plans = new ArrayList<StrategyPlan>();
        Set<String> planIds = new LinkedHashSet<String>();
        for (StrategyPlan plan : source.getStrategyPlans()) {
            if (includedColumns.containsAll(plan.getTargetColumns())) {
                plans.add(plan);
                planIds.add(plan.getStrategyId());
            }
        }
        List<StrategyExecutionResult> executions =
                new ArrayList<StrategyExecutionResult>();
        for (StrategyExecutionResult execution
                : source.getStrategyBatch().getExecutions()) {
            if (planIds.contains(execution.getSummary().getStrategyId())) {
                executions.add(execution);
            }
        }

        Map<String, FeatureDictionary> dictionaries =
                new LinkedHashMap<String, FeatureDictionary>();
        for (Map.Entry<String, FeatureDictionary> entry
                : source.getFeatures().getDictionaries().entrySet()) {
            if (includedColumns.contains(entry.getKey())) {
                dictionaries.put(entry.getKey(), entry.getValue());
            }
        }
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        for (SparseFeatureRow row : source.getFeatures().getRows()) {
            if (includedColumns.contains(row.getColumnName())) {
                rows.add(row);
            }
        }
        long retainedFeatures = 0L;
        for (FeatureDictionary dictionary : dictionaries.values()) {
            retainedFeatures += dictionary.getDefinitions().size();
        }
        FeatureAssemblyResult features = new FeatureAssemblyResult(
                dictionaries, rows, new FeatureAssemblyMetrics(rows.size(),
                retainedFeatures, retainedFeatures, 0L));

        Map<String, ColumnClusteringResult> clusteringResults =
                new LinkedHashMap<String, ColumnClusteringResult>();
        long assignmentCount = 0L;
        long clusteredColumnCount = 0L;
        for (Map.Entry<String, ColumnClusteringResult> entry
                : source.getClustering().getResults().entrySet()) {
            if (includedColumns.contains(entry.getKey())) {
                clusteringResults.put(entry.getKey(), entry.getValue());
                assignmentCount += entry.getValue().getAssignments().size();
                if (!entry.getValue().getAssignments().isEmpty()) {
                    clusteredColumnCount++;
                }
            }
        }
        ClusteringBatchResult clustering = new ClusteringBatchResult(
                clusteringResults, new ClusteringMetrics(
                clusteringResults.size(), clusteredColumnCount,
                assignmentCount,
                clusteringResults.size() - clusteredColumnCount));
        return new SnapshotPreparedArtifacts(source.getCheckpointId(), dataset,
                source.getSnapshot(), plans,
                new StrategyBatchResult(executions), features,
                source.getStrategyPlanVersion(), clustering,
                source.getConfigFingerprint());
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

    private static String key(String datasetId,
                              String snapshotId,
                              String configFingerprint) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        String fingerprint = ValueUtils.requireNotBlank(configFingerprint,
                "执行配置指纹");
        return dataset.length() + ":" + dataset
                + snapshot.length() + ":" + snapshot
                + fingerprint.length() + ":" + fingerprint;
    }
}
