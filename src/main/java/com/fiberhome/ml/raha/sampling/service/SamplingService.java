package com.fiberhome.ml.raha.sampling.service;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.AnnotationTaskRepository;
import com.fiberhome.ml.raha.sampling.ClusterCoverageScorer;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTaskStatus;
import com.fiberhome.ml.raha.sampling.domain.TupleSamplingScore;
import com.fiberhome.ml.raha.sampling.TupleSampler;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据聚类覆盖和已有标注，在预算内生成可复现且不重复的元组标注任务。
 */
public final class SamplingService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(SamplingService.class);
    /** 聚类覆盖评分器。 */
    private final ClusterCoverageScorer scorer;
    /** 加权无放回元组采样器。 */
    private final TupleSampler sampler;
    /** 采样版本生成器。 */
    private final SamplingVersioner versioner;
    /** 标注任务仓储。 */
    private final AnnotationTaskRepository repository;
    /** 提供可测试任务时间的时钟。 */
    private final Clock clock;

    public SamplingService(ClusterCoverageScorer scorer,
                           TupleSampler sampler,
                           SamplingVersioner versioner,
                           AnnotationTaskRepository repository,
                           Clock clock) {
        if (scorer == null || sampler == null || versioner == null
                || repository == null || clock == null) {
            throw new IllegalArgumentException("采样服务依赖不能为空");
        }
        this.scorer = scorer;
        this.sampler = sampler;
        this.versioner = versioner;
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 计算聚类覆盖并生成预算内标注任务，已有直接标签和任务默认不重复采样。
     *
     * @param jobId 任务标识
     * @param samplingRound 采样轮次
     * @param clustering 当前聚类结果
     * @param labels 已有标签
     * @param config 采样配置
     * @param randomSeed 可复现随机种子
     * @param version 仓储业务版本
     * @return 采样分数、任务、版本和指标
     */
    public SamplingBatchResult createTasks(String jobId,
                                           int samplingRound,
                                           ClusteringBatchResult clustering,
                                           List<CellLabel> labels,
                                           SamplingConfig config,
                                           long randomSeed,
                                           ArtifactVersion version) {
        if (clustering == null || labels == null || config == null || version == null
                || samplingRound <= 0) {
            throw new IllegalArgumentException("采样输入、配置、版本和轮次必须有效");
        }
        List<ClusterAssignment> assignments = assignments(clustering);
        List<AnnotationTask> existingTasks = repository.findByJob(jobId);
        Set<String> excludedRowIds = excludedRows(assignments, labels,
                existingTasks, config.isReviewEnabled());
        List<TupleSamplingScore> coverageScores = scorer.score(
                assignments, labels, excludedRowIds,
                config.getCoverageScoreExponentCap());
        List<TupleSamplingScore> effectiveScores = config.isClusteringBasedSampling()
                ? coverageScores : uniformScores(coverageScores);
        List<TupleSamplingScore> selected = sampler.select(effectiveScores,
                config.getLabelingBudget(), randomSeed);
        String samplingVersion = versioner.versionOf(clustering, config,
                samplingRound, randomSeed, excludedRowIds);
        long createdAt = clock.millis();
        long expiresAt = safeAdd(createdAt, config.getTaskTtlMillis());
        List<AnnotationTask> tasks = new ArrayList<AnnotationTask>(selected.size());
        for (TupleSamplingScore score : selected) {
            String taskId = HashUtils.md5Hex(jobId + "|" + samplingRound + "|"
                    + score.getRowId() + "|" + samplingVersion);
            tasks.add(new AnnotationTask(taskId, jobId, score.getRowId(), samplingRound,
                    score.getScore(), score.getCoveredClusters(), samplingVersion,
                    createdAt, expiresAt));
        }
        repository.saveAll(jobId, tasks, version, createdAt);
        SamplingMetrics metrics = new SamplingMetrics(effectiveScores.size(),
                excludedRowIds.size(), tasks.size());
        LOGGER.info("聚类覆盖采样完成，jobId={}，samplingRound={}，candidateCount={}，"
                        + "excludedCount={}，taskCount={}，randomSeed={}",
                jobId, samplingRound, metrics.getCandidateTupleCount(),
                metrics.getExcludedTupleCount(), metrics.getCreatedTaskCount(), randomSeed);
        return new SamplingBatchResult(effectiveScores, tasks, samplingVersion, metrics);
    }

    /**
     * 将已经获得人工标签的任务推进到完成状态并保存终态快照。
     *
     * @param jobId 任务标识
     * @param task 待完成标注任务
     * @param version 仓储业务版本
     * @return 完成后的任务快照
     */
    public AnnotationTask completeTask(String jobId,
                                       AnnotationTask task,
                                       ArtifactVersion version) {
        if (jobId == null || jobId.trim().isEmpty()
                || task == null || version == null
                || !jobId.equals(task.getJobId())) {
            throw new IllegalArgumentException("完成标注任务的输入和版本必须有效");
        }
        AnnotationTask completed = task.snapshot();
        long finishedAt = Math.max(completed.getCreatedAt(), clock.millis());
        completed.complete(finishedAt);
        repository.saveAll(jobId, Collections.singletonList(completed),
                version, finishedAt);
        LOGGER.info("标注任务已完成，jobId={}，taskId={}，samplingRound={}，rowId={}",
                jobId, completed.getTaskId(), completed.getSamplingRound(),
                completed.getRowId());
        return completed.snapshot();
    }

    private static List<ClusterAssignment> assignments(ClusteringBatchResult clustering) {
        List<ClusterAssignment> assignments = new ArrayList<ClusterAssignment>();
        for (ColumnClusteringResult result : clustering.getResults().values()) {
            assignments.addAll(result.getAssignments());
        }
        return assignments;
    }

    private static Set<String> excludedRows(List<ClusterAssignment> assignments,
                                            List<CellLabel> labels,
                                            List<AnnotationTask> tasks,
                                            boolean reviewEnabled) {
        Map<String, String> rowByCell = new HashMap<String, String>();
        for (ClusterAssignment assignment : assignments) {
            if (assignment.getCoordinate() != null) {
                rowByCell.put(assignment.getCellId(), assignment.getCoordinate().getRowId());
            }
        }
        Set<String> excluded = new LinkedHashSet<String>();
        if (!reviewEnabled) {
            for (CellLabel label : labels) {
                if (label != null && label.getLabelSource() != LabelSource.PROPAGATED
                        && rowByCell.containsKey(label.getCellId())) {
                    excluded.add(rowByCell.get(label.getCellId()));
                }
            }
        }
        for (AnnotationTask task : tasks) {
            // 待标注任务始终排除；非复核模式还排除所有历史终态任务。
            if (task.getStatus() == AnnotationTaskStatus.PENDING || !reviewEnabled) {
                excluded.add(task.getRowId());
            }
        }
        return excluded;
    }

    private static List<TupleSamplingScore> uniformScores(List<TupleSamplingScore> scores) {
        List<TupleSamplingScore> uniform = new ArrayList<TupleSamplingScore>(scores.size());
        for (TupleSamplingScore score : scores) {
            uniform.add(new TupleSamplingScore(score.getRowId(), 1.0d, 0.0d,
                    score.getCoveredClusters(), Collections.<String, Double>emptyMap()));
        }
        return uniform;
    }

    private static long safeAdd(long value, long increment) {
        if (Long.MAX_VALUE - value < increment) {
            throw new IllegalArgumentException("标注任务有效期导致时间溢出");
        }
        return value + increment;
    }
}
