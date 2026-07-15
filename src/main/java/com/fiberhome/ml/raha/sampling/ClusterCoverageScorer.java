package com.fiberhome.ml.raha.sampling;

import com.fiberhome.ml.raha.cluster.ClusterAssignment;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 按聚类已有直接标签数量计算元组覆盖分数。
 */
public final class ClusterCoverageScorer {

    /**
     * 低标签聚类贡献更高，一行覆盖多个低标签聚类时累加贡献。
     *
     * @param assignments 当前聚类成员
     * @param labels 已有单元格标签
     * @param excludedRowIds 不允许再次采样的行
     * @return 按行标识稳定排序的采样分数
     */
    public List<TupleSamplingScore> score(List<ClusterAssignment> assignments,
                                          List<CellLabel> labels,
                                          Set<String> excludedRowIds) {
        if (assignments == null || labels == null || excludedRowIds == null) {
            throw new IllegalArgumentException("聚类覆盖评分参数不能为空");
        }
        Map<String, ClusterAssignment> assignmentsByCell =
                new HashMap<String, ClusterAssignment>();
        Map<String, Integer> labelCounts = new HashMap<String, Integer>();
        for (ClusterAssignment assignment : assignments) {
            if (assignment == null || assignment.getCoordinate() == null) {
                throw new IllegalArgumentException("采样要求聚类成员包含单元格坐标");
            }
            assignmentsByCell.put(assignment.getCellId(), assignment);
        }
        for (CellLabel label : labels) {
            if (label == null || label.getLabelSource() == LabelSource.PROPAGATED) {
                continue;
            }
            ClusterAssignment assignment = assignmentsByCell.get(label.getCellId());
            if (assignment != null) {
                String clusterKey = clusterKey(assignment);
                Integer count = labelCounts.get(clusterKey);
                labelCounts.put(clusterKey, count == null ? 1 : count + 1);
            }
        }
        Map<String, List<ClusterAssignment>> assignmentsByRow =
                new LinkedHashMap<String, List<ClusterAssignment>>();
        List<ClusterAssignment> sortedAssignments =
                new ArrayList<ClusterAssignment>(assignments);
        Collections.sort(sortedAssignments, Comparator.comparing(
                assignment -> assignment.getCoordinate().getRowId()
                        + "|" + assignment.getColumnName()));
        for (ClusterAssignment assignment : sortedAssignments) {
            String rowId = assignment.getCoordinate().getRowId();
            if (excludedRowIds.contains(rowId)) {
                continue;
            }
            if (!assignmentsByRow.containsKey(rowId)) {
                assignmentsByRow.put(rowId, new ArrayList<ClusterAssignment>());
            }
            assignmentsByRow.get(rowId).add(assignment);
        }
        List<TupleSamplingScore> scores = new ArrayList<TupleSamplingScore>();
        for (Map.Entry<String, List<ClusterAssignment>> entry : assignmentsByRow.entrySet()) {
            Map<String, String> coveredClusters = new LinkedHashMap<String, String>();
            Map<String, Double> contributions = new LinkedHashMap<String, Double>();
            double coverageScore = 0.0d;
            for (ClusterAssignment assignment : entry.getValue()) {
                String key = clusterKey(assignment);
                int count = labelCounts.containsKey(key) ? labelCounts.get(key) : 0;
                double contribution = Math.exp(-count);
                coverageScore += contribution;
                coveredClusters.put(assignment.getColumnName(), key);
                contributions.put(assignment.getColumnName(), contribution);
            }
            // 对累积覆盖再次指数放大，与 Python demo 的 tuple_score 语义保持一致。
            double score = Math.exp(Math.min(20.0d, coverageScore));
            scores.add(new TupleSamplingScore(entry.getKey(), score, coverageScore,
                    coveredClusters, contributions));
        }
        Collections.sort(scores, Comparator.comparing(TupleSamplingScore::getRowId));
        return Collections.unmodifiableList(scores);
    }

    private static String clusterKey(ClusterAssignment assignment) {
        return assignment.getColumnName() + "|" + assignment.getClusterVersion()
                + "|" + assignment.getClusterId();
    }
}
