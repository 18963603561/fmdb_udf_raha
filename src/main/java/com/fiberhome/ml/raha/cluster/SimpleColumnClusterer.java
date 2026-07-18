package com.fiberhome.ml.raha.cluster;

import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.ValueNormalizer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 按缺失、数字和文本长度桶生成稳定簇，适用于首期轻量采样。
 */
public final class SimpleColumnClusterer implements ColumnClusterer {

    @Override
    public ClusteringResult cluster(Collection<String> values) {
        Map<String, String> clusters = new LinkedHashMap<String, String>();
        for (String value : values) {
            String normalized = ValueNormalizer.normalize(value);
            String signature;
            if (normalized.isEmpty()) {
                signature = "missing";
            } else if (ValueNormalizer.isNumeric(normalized)) {
                signature = "numeric:" + lengthBucket(normalized.length());
            } else {
                signature = "text:" + lengthBucket(normalized.length())
                        + ':' + normalized.replaceAll("[A-Za-z]", "A")
                        .replaceAll("[0-9]", "9");
            }
            clusters.put(normalized, "cluster:" + HashUtils.sha256(signature)
                    .substring(0, 16));
        }
        return new ClusteringResult(clusters);
    }

    private static int lengthBucket(int length) {
        return Math.min(10, length / 4);
    }
}
