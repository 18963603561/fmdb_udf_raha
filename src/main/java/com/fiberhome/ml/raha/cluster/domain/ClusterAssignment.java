package com.fiberhome.ml.raha.cluster.domain;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 表示一个单元格在指定列聚类版本中的所属聚类。
 */
public final class ClusterAssignment {

    /** 稳定单元格标识。 */
    private final String cellId;
    /** 单元格所属字段。 */
    private final String columnName;
    /** 单元格稳定坐标，兼容早期对象时允许为空。 */
    private final CellCoordinate coordinate;
    /** 聚类内部标识。 */
    private final String clusterId;
    /** 使用的聚类算法名称。 */
    private final String algorithm;
    /** 聚类参数和成员映射版本。 */
    private final String clusterVersion;
    /** 单元格到聚类中心或代表点的距离，无法计算时为空。 */
    private final Double distance;

    public ClusterAssignment(String cellId,
                             String columnName,
                             String clusterId,
                             String algorithm,
                             String clusterVersion,
                             Double distance) {
        this(cellId, columnName, null, clusterId, algorithm, clusterVersion, distance);
    }

    public ClusterAssignment(String cellId,
                             String columnName,
                             CellCoordinate coordinate,
                             String clusterId,
                             String algorithm,
                             String clusterVersion,
                             Double distance) {
        this.cellId = ValueUtils.requireNotBlank(cellId, "单元格标识");
        this.columnName = ValueUtils.requireNotBlank(columnName, "字段名称");
        if (coordinate != null && (!coordinate.toCellId().equals(this.cellId)
                || !coordinate.getColumnName().equals(this.columnName))) {
            throw new IllegalArgumentException("聚类单元格坐标与标识不一致");
        }
        this.coordinate = coordinate;
        this.clusterId = ValueUtils.requireNotBlank(clusterId, "聚类标识");
        this.algorithm = ValueUtils.requireNotBlank(algorithm, "聚类算法");
        this.clusterVersion = ValueUtils.requireNotBlank(clusterVersion, "聚类版本");
        if (distance != null && (Double.isNaN(distance)
                || Double.isInfinite(distance) || distance < 0.0d)) {
            throw new IllegalArgumentException("聚类距离必须为非负有限数值");
        }
        this.distance = distance;
    }

    public String getCellId() {
        return cellId;
    }

    public String getColumnName() {
        return columnName;
    }

    public CellCoordinate getCoordinate() {
        return coordinate;
    }

    public String getClusterId() {
        return clusterId;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getClusterVersion() {
        return clusterVersion;
    }

    public Double getDistance() {
        return distance;
    }
}
