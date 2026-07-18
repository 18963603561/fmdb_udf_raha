package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.core.RepositoryKey;
import com.fiberhome.ml.raha.repository.core.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.core.RepositoryRecord;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 基于统一仓储实现策略计划和执行结果的版本化持久化。
 */
public final class DefaultStrategyRepository implements StrategyRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultStrategyRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public void savePlans(String datasetId,
                          String snapshotId,
                          List<StrategyPlan> plans,
                          ArtifactVersion version,
                          long updatedAt) {
        if (plans == null || version == null) {
            throw new IllegalArgumentException("策略计划集合和版本不能为空");
        }
        String partition = snapshotPartition(datasetId, snapshotId);
        repository.executeInTransaction(transactionRepository -> {
            for (StrategyPlan plan : plans) {
                transactionRepository.save(new RepositoryRecord<StrategyPlan>(
                        new RepositoryKey(RepositoryNamespace.STRATEGY_PLAN,
                                partition, plan.getStrategyId()),
                        version, plan, updatedAt));
            }
        });
    }

    @Override
    public List<StrategyPlan> findPlans(String datasetId, String snapshotId) {
        List<RepositoryRecord<StrategyPlan>> records = repository.findByPartition(
                RepositoryNamespace.STRATEGY_PLAN, snapshotPartition(datasetId, snapshotId),
                StrategyPlan.class);
        List<StrategyPlan> plans = new ArrayList<StrategyPlan>(records.size());
        for (RepositoryRecord<StrategyPlan> record : records) {
            plans.add(record.getPayload());
        }
        Collections.sort(plans, new Comparator<StrategyPlan>() {
            @Override
            public int compare(StrategyPlan first, StrategyPlan second) {
                int priorityCompare = Integer.compare(first.getPriority(), second.getPriority());
                return priorityCompare == 0
                        ? first.getStrategyId().compareTo(second.getStrategyId())
                        : priorityCompare;
            }
        });
        return Collections.unmodifiableList(plans);
    }

    @Override
    public void saveExecution(StrategyExecutionResult result,
                              ArtifactVersion version,
                              long updatedAt) {
        if (result == null || version == null) {
            throw new IllegalArgumentException("策略执行结果和版本不能为空");
        }
        StrategyRunSummary summary = result.getSummary();
        String jobId = summary.getJobId();
        repository.executeInTransaction(transactionRepository -> {
            transactionRepository.save(new RepositoryRecord<StrategyRunSummary>(
                    new RepositoryKey(RepositoryNamespace.STRATEGY_RUN_SUMMARY,
                            jobId, summary.getStrategyId()),
                    version, summary, updatedAt));
            for (StrategyHit hit : result.getHits()) {
                String recordKey = hit.getStrategyId() + ":"
                        + hit.getCoordinate().toCellId() + ":" + hit.getReasonCode();
                transactionRepository.save(new RepositoryRecord<StrategyHit>(
                        new RepositoryKey(RepositoryNamespace.STRATEGY_HIT,
                                jobId, recordKey),
                        version, hit, updatedAt));
            }
        });
    }

    @Override
    public List<StrategyHit> findHits(String jobId) {
        return payloads(repository.findByPartition(RepositoryNamespace.STRATEGY_HIT,
                ValueUtils.requireNotBlank(jobId, "任务标识"), StrategyHit.class));
    }

    @Override
    public List<StrategyRunSummary> findSummaries(String jobId) {
        return payloads(repository.findByPartition(RepositoryNamespace.STRATEGY_RUN_SUMMARY,
                ValueUtils.requireNotBlank(jobId, "任务标识"), StrategyRunSummary.class));
    }

    private static String snapshotPartition(String datasetId, String snapshotId) {
        String validatedDatasetId = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String validatedSnapshotId = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        return validatedDatasetId.length() + ":" + validatedDatasetId
                + validatedSnapshotId.length() + ":" + validatedSnapshotId;
    }

    private static <T> List<T> payloads(List<RepositoryRecord<T>> records) {
        List<T> payloads = new ArrayList<T>(records.size());
        for (RepositoryRecord<T> record : records) {
            payloads.add(record.getPayload());
        }
        return Collections.unmodifiableList(payloads);
    }
}
