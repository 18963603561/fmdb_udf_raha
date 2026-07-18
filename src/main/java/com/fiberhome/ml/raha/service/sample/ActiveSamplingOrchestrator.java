package com.fiberhome.ml.raha.service.sample;

import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.config.dto.ResourceConfig;
import com.fiberhome.ml.raha.config.dto.SamplingConfig;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.data.type.JobStatus;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按一轮一个元组执行聚类覆盖主动采样，并在每轮标注后更新覆盖状态。
 */
public final class ActiveSamplingOrchestrator {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ActiveSamplingOrchestrator.class);
    /** 单轮聚类和采样服务。 */
    private final RahaSampleService sampleService;

    public ActiveSamplingOrchestrator(RahaSampleService sampleService) {
        if (sampleService == null) {
            throw new IllegalArgumentException("主动采样编排器依赖不能为空");
        }
        this.sampleService = sampleService;
    }

    /**
     * 逐轮采样并获取直接标签，直到达到总标注预算。
     *
     * @param jobId 采样任务标识
     * @param features 已准备单元格特征
     * @param initialLabels 已有直接标签
     * @param clusteringConfig 聚类配置
     * @param samplingConfig 采样配置
     * @param totalBudget 总标注行预算
     * @param randomSeed 稳定随机种子
     * @param artifactVersion 仓储业务版本
     * @param resourceConfig 并行和阶段超时配置
     * @param labelProvider 抽中元组后的直接标签提供器
     * @return 完整逐轮采样结果
     */
    public ActiveSamplingResult sample(String jobId,
                                       FeatureAssemblyResult features,
                                       List<CellLabel> initialLabels,
                                       ClusteringConfig clusteringConfig,
                                       SamplingConfig samplingConfig,
                                       int totalBudget,
                                       long randomSeed,
                                       ArtifactVersion artifactVersion,
                                       ResourceConfig resourceConfig,
                                       SampledTupleLabelProvider labelProvider) {
        if (jobId == null || jobId.trim().isEmpty() || features == null
                || initialLabels == null || clusteringConfig == null
                || samplingConfig == null || totalBudget <= 0
                || artifactVersion == null || resourceConfig == null
                || labelProvider == null) {
            throw new IllegalArgumentException("主动采样输入、预算和依赖必须有效");
        }
        SamplingConfig roundConfig = new SamplingConfig(1,
                samplingConfig.isClusteringBasedSampling(), false,
                samplingConfig.getTaskTtlMillis(),
                samplingConfig.getCoverageScoreExponentCap());
        List<CellLabel> labels = new ArrayList<CellLabel>(initialLabels);
        List<AnnotationTask> completedTasks = new ArrayList<AnnotationTask>();
        List<String> selectedRows = new ArrayList<String>();
        Set<String> selectedCellIds = new LinkedHashSet<String>();
        ClusteringBatchResult reusableClustering = null;
        int effectiveBudget = effectiveBudget(features, initialLabels, totalBudget);
        if (effectiveBudget <= 0) {
            throw new IllegalStateException("主动采样没有可用候选行");
        }
        if (effectiveBudget < totalBudget) {
            LOGGER.warn("主动采样预算超过可用候选行，自动收缩预算，jobId={}，"
                            + "requestedBudget={}，effectiveBudget={}",
                    jobId, totalBudget, effectiveBudget);
        }
        LOGGER.info("开始逐轮主动采样，jobId={}，requestedBudget={}，"
                        + "effectiveBudget={}，randomSeed={}",
                jobId, totalBudget, effectiveBudget, randomSeed);
        for (int round = 1; round <= effectiveBudget; round++) {
            RahaServiceResult<RahaSampleOutput> result = sampleService.sample(
                    new RahaSampleRequest(jobId, features, labels,
                            clusteringConfig, roundConfig, round, randomSeed,
                            artifactVersion, resourceConfig, reusableClustering));
            if (result.getStatus() == JobStatus.FAILED
                    || result.getPayload() == null
                    || result.getPayload().getSampling().getTasks().size() != 1) {
                throw new IllegalStateException("主动采样第 " + round
                        + " 轮未生成唯一标注任务");
            }
            reusableClustering = result.getPayload().getClustering();
            AnnotationTask task = result.getPayload().getSampling().getTasks().get(0);
            List<CellLabel> roundLabels = labelProvider.labelsFor(task);
            validateRoundLabels(task, roundLabels, selectedCellIds);
            labels.addAll(roundLabels);
            selectedRows.add(task.getRowId());
            for (CellLabel label : roundLabels) {
                selectedCellIds.add(label.getCellId());
            }
            completedTasks.add(sampleService.completeTask(
                    jobId, task, artifactVersion));
            LOGGER.info("主动采样轮次完成，jobId={}，round={}，rowId={}，"
                            + "roundLabelCount={}，accumulatedLabelCount={}",
                    jobId, round, task.getRowId(), roundLabels.size(), labels.size());
        }
        LOGGER.info("逐轮主动采样完成，jobId={}，sampledRowCount={}，directLabelCount={}",
                jobId, selectedRows.size(), labels.size());
        return new ActiveSamplingResult(completedTasks, selectedRows,
                labels, selectedCellIds, reusableClustering);
    }

    /**
     * 根据稳定特征坐标和已有直接标签计算本次可执行的最大采样预算。
     *
     * @param features 已准备单元格特征
     * @param labels 已有标签
     * @param requestedBudget 请求预算
     * @return 不超过剩余候选行数量的有效预算
     */
    static int effectiveBudget(FeatureAssemblyResult features,
                               List<CellLabel> labels,
                               int requestedBudget) {
        if (features == null || labels == null || requestedBudget <= 0) {
            throw new IllegalArgumentException("主动采样预算计算输入必须有效");
        }
        Set<String> directCellIds = new LinkedHashSet<String>();
        for (CellLabel label : labels) {
            if (label != null && label.getLabelSource() != LabelSource.PROPAGATED) {
                directCellIds.add(label.getCellId());
            }
        }
        Set<String> candidateRows = new LinkedHashSet<String>();
        Set<String> excludedRows = new LinkedHashSet<String>();
        for (SparseFeatureRow row : features.getRows()) {
            if (row != null && row.getCoordinate() != null) {
                String rowId = row.getCoordinate().getRowId();
                candidateRows.add(rowId);
                if (directCellIds.contains(row.getCellId())) {
                    excludedRows.add(rowId);
                }
            }
        }
        candidateRows.removeAll(excludedRows);
        return Math.min(requestedBudget, candidateRows.size());
    }

    private static void validateRoundLabels(AnnotationTask task,
                                            List<CellLabel> labels,
                                            Set<String> existingCellIds) {
        if (labels == null || labels.isEmpty()) {
            throw new IllegalArgumentException("主动采样任务必须返回直接标签");
        }
        for (CellLabel label : labels) {
            if (label == null || label.getLabelSource() == LabelSource.PROPAGATED
                    || existingCellIds.contains(label.getCellId())) {
                throw new IllegalArgumentException("主动采样标签为空、重复或来源非法");
            }
        }
    }
}
