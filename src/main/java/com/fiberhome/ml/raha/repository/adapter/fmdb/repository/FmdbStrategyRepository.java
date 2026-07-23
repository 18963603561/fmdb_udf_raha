package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbStrategyArtifactCodec;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 暂存策略命中，并从最终训练列级产物恢复计划和执行摘要。
 */
public final class FmdbStrategyRepository implements StrategyRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbStrategyRepository.class);
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 当前任务尚未统一物化的策略计划。 */
    private final Map<String, List<StrategyPlan>> pendingPlans =
            new LinkedHashMap<String, List<StrategyPlan>>();
    /** 策略命中仅在特征生成前使用，不进入最终物理表。 */
    private final Map<String, Map<String, StrategyHit>> pendingHits =
            new LinkedHashMap<String, Map<String, StrategyHit>>();
    /** 当前任务尚未统一物化的策略摘要。 */
    private final Map<String, Map<String, StrategyRunSummary>> pendingSummaries =
            new LinkedHashMap<String, Map<String, StrategyRunSummary>>();
    /** 训练列级产物表名。 */
    private final String tableName;

    public FmdbStrategyRepository(FmdbTableGateway tableGateway,
                                  FmdbPersistenceConfig persistenceConfig) {
        if (tableGateway == null || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 策略仓储依赖不能为空");
        }
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT.getTableName();
    }

    @Override
    public synchronized void savePlans(String datasetId,
                                       String snapshotId,
                                       List<StrategyPlan> plans,
                                       ArtifactVersion version,
                                       long updatedAt) {
        if (plans == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("策略计划、版本和更新时间必须有效");
        }
        pendingPlans.put(snapshotKey(datasetId, snapshotId),
                immutablePlans(plans));
        LOGGER.info("策略计划阶段保存完成，datasetId={}，snapshotId={}，planCount={}",
                datasetId, snapshotId, plans.size());
    }

    @Override
    public synchronized List<StrategyPlan> findPlans(String datasetId,
                                                     String snapshotId) {
        List<StrategyPlan> pending = pendingPlans.get(snapshotKey(datasetId, snapshotId));
        if (pending != null) {
            return pending;
        }
        Map<String, StrategyPlan> plans = new LinkedHashMap<String, StrategyPlan>();
        for (Row row : artifactRowsBySnapshot(datasetId, snapshotId)) {
            for (StrategyPlan plan : FmdbStrategyArtifactCodec.readPlans(
                    (String) row.getAs("strategy_plan_json"))) {
                plans.put(plan.getStrategyId(), plan);
            }
        }
        return immutablePlans(new ArrayList<StrategyPlan>(plans.values()));
    }

    @Override
    public synchronized void saveExecution(StrategyExecutionResult result,
                                           ArtifactVersion version,
                                           long updatedAt) {
        if (result == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("策略执行结果、版本和更新时间必须有效");
        }
        StrategyRunSummary summary = result.getSummary();
        if (!pendingSummaries.containsKey(summary.getJobId())) {
            pendingSummaries.put(summary.getJobId(),
                    new LinkedHashMap<String, StrategyRunSummary>());
        }
        pendingSummaries.get(summary.getJobId()).put(summary.getStrategyId(), summary);
        if (!pendingHits.containsKey(summary.getJobId())) {
            pendingHits.put(summary.getJobId(), new LinkedHashMap<String, StrategyHit>());
        }
        for (StrategyHit hit : result.getHits()) {
            String key = hit.getStrategyId() + ":" + hit.getCoordinate().toCellId()
                    + ":" + hit.getReasonCode();
            pendingHits.get(summary.getJobId()).put(key, hit);
        }
        LOGGER.debug("策略执行结果已进入统一物化缓冲，jobId={}，strategyId={}，hitCount={}",
                summary.getJobId(), summary.getStrategyId(), result.getHits().size());
    }

    @Override
    public synchronized List<StrategyHit> findHits(String jobId) {
        String validated = ValueUtils.requireNotBlank(jobId, "任务标识");
        Map<String, StrategyHit> hits = pendingHits.get(validated);
        // 设计上策略命中只在特征生成前短暂存在，不写入最终九表。
        return hits == null ? Collections.<StrategyHit>emptyList()
                : Collections.unmodifiableList(new ArrayList<StrategyHit>(hits.values()));
    }

    @Override
    public synchronized List<StrategyRunSummary> findSummaries(String jobId) {
        String validated = ValueUtils.requireNotBlank(jobId, "任务标识");
        Map<String, StrategyRunSummary> summaries =
                new LinkedHashMap<String, StrategyRunSummary>();
        for (Row row : artifactRowsByBatch(validated)) {
            for (StrategyRunSummary summary : FmdbStrategyArtifactCodec.readSummaries(
                    (String) row.getAs("strategy_plan_json"))) {
                summaries.put(summary.getStrategyId(), summary);
            }
        }
        Map<String, StrategyRunSummary> pending = pendingSummaries.get(validated);
        if (pending != null) {
            summaries.putAll(pending);
        }
        List<StrategyRunSummary> result =
                new ArrayList<StrategyRunSummary>(summaries.values());
        Collections.sort(result, Comparator.comparing(
                StrategyRunSummary::getStrategyId));
        return Collections.unmodifiableList(result);
    }

    @Override
    public synchronized void releaseHits(String jobId,
                                         Set<String> strategyIds) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (strategyIds == null) {
            throw new IllegalArgumentException("待释放策略标识不能为空");
        }
        Map<String, StrategyHit> hits = pendingHits.get(validatedJobId);
        if (hits == null) {
            return;
        }
        Iterator<Map.Entry<String, StrategyHit>> iterator =
                hits.entrySet().iterator();
        while (iterator.hasNext()) {
            if (strategyIds.contains(
                    iterator.next().getValue().getStrategyId())) {
                iterator.remove();
            }
        }
        if (hits.isEmpty()) {
            pendingHits.remove(validatedJobId);
        }
        LOGGER.debug("任务级策略命中缓存已释放，jobId={}，strategyCount={}",
                validatedJobId, strategyIds.size());
    }

    private List<Row> artifactRowsBySnapshot(String datasetId, String snapshotId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        LOGGER.debug("训练策略计划仅使用当前任务缓存，未命中缓存，datasetId={}，snapshotId={}",
                dataset, snapshot);
        return Collections.emptyList();
    }

    private List<Row> artifactRowsByBatch(String jobId) {
        String validated = ValueUtils.requireNotBlank(jobId, "任务标识");
        LOGGER.debug("训练策略计划仅使用当前任务缓存，未命中缓存，jobId={}", validated);
        return Collections.emptyList();
    }

    private static List<StrategyPlan> immutablePlans(List<StrategyPlan> source) {
        List<StrategyPlan> plans = new ArrayList<StrategyPlan>(source);
        Collections.sort(plans, new Comparator<StrategyPlan>() {
            @Override
            public int compare(StrategyPlan first, StrategyPlan second) {
                int priority = Integer.compare(first.getPriority(), second.getPriority());
                return priority == 0 ? first.getStrategyId().compareTo(
                        second.getStrategyId()) : priority;
            }
        });
        return Collections.unmodifiableList(plans);
    }

    private static String snapshotKey(String datasetId, String snapshotId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        return dataset.length() + ":" + dataset + snapshot.length() + ":" + snapshot;
    }
}
