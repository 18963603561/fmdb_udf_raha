package com.fiberhome.ml.raha.label.propagation;

import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 记录单个聚类的直接标签分布、冲突比例和传播数量。
 */
public final class ClusterPropagationSummary {

    /** 传播摘要稳定标识。 */
    private final String summaryId;
    /** 字段名称。 */
    private final String columnName;
    /** 聚类标识。 */
    private final String clusterId;
    /** 聚类版本。 */
    private final String clusterVersion;
    /** 传播方式。 */
    private final LabelPropagationMethod method;
    /** 传播状态。 */
    private final ClusterPropagationStatus status;
    /** 直接标签数量。 */
    private final int directLabelCount;
    /** 直接错误标签数量。 */
    private final int errorLabelCount;
    /** 直接正常标签数量。 */
    private final int normalLabelCount;
    /** 少数或冲突标签数量。 */
    private final int conflictCount;
    /** 多数比例，没有直接标签时为空。 */
    private final Double majorityRatio;
    /** 新增传播标签数量。 */
    private final int propagatedLabelCount;

    public ClusterPropagationSummary(String columnName,
                                     String clusterId,
                                     String clusterVersion,
                                     LabelPropagationMethod method,
                                     ClusterPropagationStatus status,
                                     int directLabelCount,
                                     int errorLabelCount,
                                     int normalLabelCount,
                                     int conflictCount,
                                     Double majorityRatio,
                                     int propagatedLabelCount) {
        this.columnName = ValueUtils.requireNotBlank(columnName, "字段名称");
        this.clusterId = ValueUtils.requireNotBlank(clusterId, "聚类标识");
        this.clusterVersion = ValueUtils.requireNotBlank(clusterVersion, "聚类版本");
        if (method == null || status == null || directLabelCount < 0
                || errorLabelCount < 0 || normalLabelCount < 0
                || conflictCount < 0 || propagatedLabelCount < 0
                || errorLabelCount + normalLabelCount != directLabelCount
                || (majorityRatio != null && (Double.isNaN(majorityRatio)
                || majorityRatio < 0.5d || majorityRatio > 1.0d))) {
            throw new IllegalArgumentException("传播摘要参数非法");
        }
        this.method = method;
        this.status = status;
        this.directLabelCount = directLabelCount;
        this.errorLabelCount = errorLabelCount;
        this.normalLabelCount = normalLabelCount;
        this.conflictCount = conflictCount;
        this.majorityRatio = majorityRatio;
        this.propagatedLabelCount = propagatedLabelCount;
        this.summaryId = HashUtils.md5Hex(columnName + "|" + clusterVersion + "|"
                + clusterId + "|" + method.name());
    }

    public String getSummaryId() { return summaryId; }
    public String getColumnName() { return columnName; }
    public String getClusterId() { return clusterId; }
    public String getClusterVersion() { return clusterVersion; }
    public LabelPropagationMethod getMethod() { return method; }
    public ClusterPropagationStatus getStatus() { return status; }
    public int getDirectLabelCount() { return directLabelCount; }
    public int getErrorLabelCount() { return errorLabelCount; }
    public int getNormalLabelCount() { return normalLabelCount; }
    public int getConflictCount() { return conflictCount; }
    public Double getMajorityRatio() { return majorityRatio; }
    public int getPropagatedLabelCount() { return propagatedLabelCount; }
}
