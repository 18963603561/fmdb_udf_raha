package com.fiberhome.ml.raha.cluster;

import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.util.HashUtils;
import java.util.Map;
import java.util.TreeMap;

/**
 * 根据特征字典、聚类参数、状态和成员关系生成稳定版本。
 */
public final class ClusterVersioner {

    /**
     * 生成包含最终成员关系的稳定聚类版本。
     *
     * @param columnName 字段名称
     * @param dictionaryVersion 特征字典版本
     * @param algorithm 聚类算法名称
     * @param config 聚类配置
     * @param randomSeed 随机种子
     * @param status 聚类结果状态
     * @param memberships 单元格到聚类的成员关系
     * @return MD5 聚类版本
     */
    public String versionOf(String columnName,
                            String dictionaryVersion,
                            String algorithm,
                            ClusteringConfig config,
                            long randomSeed,
                            ColumnClusteringStatus status,
                            Map<String, String> memberships) {
        if (config == null || status == null || memberships == null) {
            throw new IllegalArgumentException("聚类版本参数不能为空");
        }
        StringBuilder canonical = new StringBuilder();
        canonical.append(columnName).append('|').append(dictionaryVersion).append('|')
                .append(algorithm).append('|').append(config.getDistanceMetric()).append('|')
                .append(config.getTargetClusterCount()).append('|')
                .append(config.getMaxSampleCount()).append('|').append(randomSeed).append('|')
                .append(status.name());
        for (Map.Entry<String, String> entry
                : new TreeMap<String, String>(memberships).entrySet()) {
            canonical.append('|').append(entry.getKey()).append('=').append(entry.getValue());
        }
        return HashUtils.md5Hex(canonical.toString());
    }
}
