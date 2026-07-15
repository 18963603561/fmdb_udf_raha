package com.fiberhome.ml.raha.sampling;

import com.fiberhome.ml.raha.cluster.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.ColumnClusteringResult;
import com.fiberhome.ml.raha.config.SamplingConfig;
import com.fiberhome.ml.raha.util.HashUtils;

import java.util.Set;
import java.util.TreeSet;

/**
 * 根据聚类版本、轮次、预算、随机种子和排除集合生成稳定采样版本。
 */
public final class SamplingVersioner {

    /**
     * 生成包含聚类版本、预算和排除集合的稳定采样版本。
     *
     * @param clustering 当前聚类结果
     * @param config 采样配置
     * @param samplingRound 采样轮次
     * @param randomSeed 随机种子
     * @param excludedRowIds 已标注或已有任务的排除行
     * @return SHA-256 采样版本
     */
    public String versionOf(ClusteringBatchResult clustering,
                            SamplingConfig config,
                            int samplingRound,
                            long randomSeed,
                            Set<String> excludedRowIds) {
        if (clustering == null || config == null || excludedRowIds == null
                || samplingRound <= 0) {
            throw new IllegalArgumentException("采样版本参数不能为空且轮次必须大于 0");
        }
        StringBuilder canonical = new StringBuilder();
        canonical.append(samplingRound).append('|').append(randomSeed).append('|')
                .append(config.getLabelingBudget()).append('|')
                .append(config.isClusteringBasedSampling()).append('|')
                .append(config.isReviewEnabled()).append('|')
                .append(config.getTaskTtlMillis());
        for (String column : new TreeSet<String>(clustering.getResults().keySet())) {
            ColumnClusteringResult result = clustering.getResults().get(column);
            canonical.append('|').append(column).append('=').append(result.getClusterVersion());
        }
        for (String rowId : new TreeSet<String>(excludedRowIds)) {
            canonical.append("|excluded=").append(rowId);
        }
        return HashUtils.sha256Hex(canonical.toString());
    }
}
