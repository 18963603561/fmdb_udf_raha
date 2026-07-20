package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 定义聚类摘要在列级产物表中的稳定 JSON 往返协议。
 */
public final class FmdbClusterSummaryCodec {

    private FmdbClusterSummaryCodec() {
    }

    public static String write(ColumnClusteringResult result) {
        if (result == null) {
            throw new IllegalArgumentException("聚类结果不能为空");
        }
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("algorithm", result.getAlgorithm());
        values.put("distanceMetric", result.getDistanceMetric().name());
        values.put("requestedClusterCount", result.getRequestedClusterCount());
        values.put("effectiveClusterCount", result.getEffectiveClusterCount());
        values.put("randomSeed", result.getRandomSeed());
        values.put("assignmentCount", result.getAssignments().size());
        values.put("status", result.getStatus().name());
        values.put("message", result.getMessage());
        values.put("createdAt", result.getCreatedAt());
        return FmdbJsonCodec.write(values);
    }

    public static ColumnClusteringResult read(String columnName,
                                              String clusterVersion,
                                              String json,
                                              List<ClusterAssignment> assignments) {
        Map<String, Object> values = FmdbJsonCodec.readObject(json);
        return new ColumnClusteringResult(columnName,
                FmdbJsonValue.requiredText(values, "algorithm"),
                ClusteringDistanceMetric.valueOf(FmdbJsonValue.requiredText(
                        values, "distanceMetric")),
                FmdbJsonValue.requiredNumber(values, "requestedClusterCount").intValue(),
                FmdbJsonValue.requiredNumber(values, "effectiveClusterCount").intValue(),
                FmdbJsonValue.requiredNumber(values, "randomSeed").longValue(),
                clusterVersion,
                ColumnClusteringStatus.valueOf(FmdbJsonValue.requiredText(values, "status")),
                FmdbJsonValue.optionalText(values, "message"), assignments,
                FmdbJsonValue.requiredNumber(values, "createdAt").longValue());
    }
}
