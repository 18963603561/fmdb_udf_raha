package com.fiberhome.ml.raha.cluster.domain;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存单列聚类状态、版本、参数和成员映射。
 */
public final class ColumnClusteringResult {

    /** 聚类字段。 */
    private final String columnName;
    /** 聚类算法名称。 */
    private final String algorithm;
    /** 距离度量。 */
    private final ClusteringDistanceMetric distanceMetric;
    /** 配置请求的簇数量。 */
    private final int requestedClusterCount;
    /** 实际生成的簇数量。 */
    private final int effectiveClusterCount;
    /** 聚类使用的随机种子。 */
    private final long randomSeed;
    /** 聚类成员和参数版本。 */
    private final String clusterVersion;
    /** 可解释执行状态。 */
    private final ColumnClusteringStatus status;
    /** 不包含原始值的状态说明。 */
    private final String message;
    /** 单元格到聚类的成员映射。 */
    private final List<ClusterAssignment> assignments;
    /** 结果创建时间。 */
    private final long createdAt;

    public ColumnClusteringResult(String columnName,
                                  String algorithm,
                                  ClusteringDistanceMetric distanceMetric,
                                  int requestedClusterCount,
                                  int effectiveClusterCount,
                                  long randomSeed,
                                  String clusterVersion,
                                  ColumnClusteringStatus status,
                                  String message,
                                  List<ClusterAssignment> assignments,
                                  long createdAt) {
        this.columnName = ValueUtils.requireNotBlank(columnName, "聚类字段");
        this.algorithm = ValueUtils.requireNotBlank(algorithm, "聚类算法");
        if (distanceMetric == null || status == null) {
            throw new IllegalArgumentException("聚类距离和状态不能为空");
        }
        if (requestedClusterCount <= 0 || effectiveClusterCount < 0) {
            throw new IllegalArgumentException("聚类请求数量和实际数量非法");
        }
        this.distanceMetric = distanceMetric;
        this.requestedClusterCount = requestedClusterCount;
        this.effectiveClusterCount = effectiveClusterCount;
        this.randomSeed = randomSeed;
        this.clusterVersion = ValueUtils.requireNotBlank(clusterVersion, "聚类版本");
        this.status = status;
        this.message = message;
        List<ClusterAssignment> copies = assignments == null
                ? Collections.<ClusterAssignment>emptyList()
                : new ArrayList<ClusterAssignment>(assignments);
        for (ClusterAssignment assignment : copies) {
            if (assignment == null || !this.columnName.equals(assignment.getColumnName())
                    || !this.clusterVersion.equals(assignment.getClusterVersion())) {
                throw new IllegalArgumentException("聚类成员与结果字段或版本不一致");
            }
        }
        this.assignments = Collections.unmodifiableList(copies);
        if (createdAt <= 0L) {
            throw new IllegalArgumentException("聚类结果创建时间必须大于 0");
        }
        this.createdAt = createdAt;
    }

    public String getColumnName() { return columnName; }
    public String getAlgorithm() { return algorithm; }
    public ClusteringDistanceMetric getDistanceMetric() { return distanceMetric; }
    public int getRequestedClusterCount() { return requestedClusterCount; }
    public int getEffectiveClusterCount() { return effectiveClusterCount; }
    public long getRandomSeed() { return randomSeed; }
    public String getClusterVersion() { return clusterVersion; }
    public ColumnClusteringStatus getStatus() { return status; }
    public String getMessage() { return message; }
    public List<ClusterAssignment> getAssignments() { return assignments; }
    public long getCreatedAt() { return createdAt; }
}
