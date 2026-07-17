package com.fiberhome.ml.raha.cluster;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 单列轻量聚类结果，保存值到稳定簇标识的映射。
 */
public final class ClusteringResult {

    /** 值到簇标识的映射。 */
    private final Map<String, String> clusterByValue;

    public ClusteringResult(Map<String, String> clusterByValue) {
        this.clusterByValue = Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(clusterByValue));
    }

    public Map<String, String> getClusterByValue() {
        return clusterByValue;
    }
}
