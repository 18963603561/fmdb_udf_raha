package com.fiberhome.ml.raha.cluster.domain;

/**
 * 汇总列内聚类数量、成员数量和异常列数量。
 */
public final class ClusteringMetrics {

    /** 参与聚类的字段数量。 */
    private final long columnCount;
    /** 成功生成成员的字段数量。 */
    private final long clusteredColumnCount;
    /** 聚类成员总数。 */
    private final long assignmentCount;
    /** 返回空结果或异常状态的字段数量。 */
    private final long exceptionalColumnCount;

    public ClusteringMetrics(long columnCount,
                             long clusteredColumnCount,
                             long assignmentCount,
                             long exceptionalColumnCount) {
        if (columnCount < 0L || clusteredColumnCount < 0L
                || assignmentCount < 0L || exceptionalColumnCount < 0L) {
            throw new IllegalArgumentException("聚类指标不能为负数");
        }
        this.columnCount = columnCount;
        this.clusteredColumnCount = clusteredColumnCount;
        this.assignmentCount = assignmentCount;
        this.exceptionalColumnCount = exceptionalColumnCount;
    }

    public long getColumnCount() { return columnCount; }
    public long getClusteredColumnCount() { return clusteredColumnCount; }
    public long getAssignmentCount() { return assignmentCount; }
    public long getExceptionalColumnCount() { return exceptionalColumnCount; }
}
