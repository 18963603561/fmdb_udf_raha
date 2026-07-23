package com.fiberhome.ml.raha.sampling;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.sampling.domain.TupleSamplingScore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 将分批聚类成员折叠为按行保存的紧凑采样覆盖状态。
 *
 * <p>每个字段只处理一次，因此批次结束后可以释放全部单元格聚类对象。
 * 累积器只保留行标识、字段簇键和字段贡献。</p>
 */
public final class ClusterCoverageAccumulator {

    /** 非传播标签对应的单元格标识。 */
    private final Set<String> directLabelCellIds =
            new LinkedHashSet<String>();
    /** 已直接标注的行标识。 */
    private final Set<String> directlyLabeledRows =
            new LinkedHashSet<String>();
    /** 按行标识累积的覆盖状态。 */
    private final Map<String, RowCoverage> coverageByRow =
            new LinkedHashMap<String, RowCoverage>();
    /** 按字段保存的聚类版本，用于生成稳定采样版本。 */
    private final Map<String, String> clusterVersions =
            new LinkedHashMap<String, String>();
    /** 复用相同簇键字符串，避免每个单元格重复分配。 */
    private final Map<String, String> canonicalClusterKeys =
            new HashMap<String, String>();
    /** 覆盖分数指数上限。 */
    private final double exponentCap;

    public ClusterCoverageAccumulator(List<CellLabel> labels,
                                      double exponentCap) {
        if (labels == null || Double.isNaN(exponentCap)
                || Double.isInfinite(exponentCap) || exponentCap <= 0.0d) {
            throw new IllegalArgumentException("紧凑采样覆盖参数非法");
        }
        for (CellLabel label : labels) {
            if (label != null
                    && label.getLabelSource() != LabelSource.PROPAGATED) {
                directLabelCellIds.add(label.getCellId());
            }
        }
        this.exponentCap = exponentCap;
    }

    /**
     * 累积一个逻辑列批的聚类覆盖，并在返回后允许调用方释放该批对象。
     *
     * @param clustering 当前列批聚类结果
     */
    public void addBatch(ClusteringBatchResult clustering) {
        if (clustering == null) {
            throw new IllegalArgumentException("列批聚类结果不能为空");
        }
        List<ClusterAssignment> assignments =
                new ArrayList<ClusterAssignment>();
        for (ColumnClusteringResult result : clustering.getResults().values()) {
            if (clusterVersions.put(result.getColumnName(),
                    result.getClusterVersion()) != null) {
                throw new IllegalArgumentException("采样覆盖字段被重复累积："
                        + result.getColumnName());
            }
            assignments.addAll(result.getAssignments());
        }
        Map<String, Integer> directLabelCounts =
                new HashMap<String, Integer>();
        for (ClusterAssignment assignment : assignments) {
            requireCoordinate(assignment);
            if (directLabelCellIds.contains(assignment.getCellId())) {
                String key = clusterKey(assignment);
                Integer count = directLabelCounts.get(key);
                directLabelCounts.put(key, count == null ? 1 : count + 1);
                directlyLabeledRows.add(
                        assignment.getCoordinate().getRowId());
            }
        }
        Collections.sort(assignments, Comparator.comparing(
                assignment -> assignment.getCoordinate().getRowId()
                        + "|" + assignment.getColumnName()));
        for (ClusterAssignment assignment : assignments) {
            String rowId = assignment.getCoordinate().getRowId();
            if (!coverageByRow.containsKey(rowId)) {
                coverageByRow.put(rowId, new RowCoverage());
            }
            String key = canonicalClusterKey(assignment);
            int count = directLabelCounts.containsKey(key)
                    ? directLabelCounts.get(key) : 0;
            coverageByRow.get(rowId).add(assignment.getColumnName(), key,
                    Math.exp(-count));
        }
    }

    /**
     * 生成按行标识稳定排序的最终覆盖分数。
     */
    public List<TupleSamplingScore> scores() {
        List<TupleSamplingScore> scores =
                new ArrayList<TupleSamplingScore>(coverageByRow.size());
        for (Map.Entry<String, RowCoverage> entry : coverageByRow.entrySet()) {
            RowCoverage coverage = entry.getValue();
            scores.add(new TupleSamplingScore(entry.getKey(),
                    Math.exp(Math.min(exponentCap, coverage.coverageScore)),
                    coverage.coverageScore,
                    Collections.<String, String>emptyMap(),
                    Collections.<String, Double>emptyMap()));
        }
        Collections.sort(scores,
                Comparator.comparing(TupleSamplingScore::getRowId));
        return Collections.unmodifiableList(scores);
    }

    public Set<String> getDirectlyLabeledRows() {
        return Collections.unmodifiableSet(
                new LinkedHashSet<String>(directlyLabeledRows));
    }

    public int getCandidateRowCount() {
        return coverageByRow.size();
    }

    public Map<String, String> getClusterVersions() {
        return Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(clusterVersions));
    }

    /**
     * 只为最终选中的采样行返回字段聚类覆盖。
     */
    public Map<String, String> coveredClusters(String rowId) {
        RowCoverage coverage = coverageByRow.get(rowId);
        if (coverage == null) {
            throw new IllegalArgumentException("采样行不存在覆盖状态：" + rowId);
        }
        return Collections.unmodifiableMap(
                new LinkedHashMap<String, String>(coverage.coveredClusters));
    }

    private static void requireCoordinate(ClusterAssignment assignment) {
        if (assignment == null || assignment.getCoordinate() == null) {
            throw new IllegalArgumentException("采样覆盖要求聚类成员包含坐标");
        }
    }

    private static String clusterKey(ClusterAssignment assignment) {
        return assignment.getColumnName() + "|" + assignment.getClusterVersion()
                + "|" + assignment.getClusterId();
    }

    private String canonicalClusterKey(ClusterAssignment assignment) {
        String key = clusterKey(assignment);
        String canonical = canonicalClusterKeys.get(key);
        if (canonical == null) {
            canonicalClusterKeys.put(key, key);
            canonical = key;
        }
        return canonical;
    }

    /** 单行的字段聚类覆盖状态。 */
    private static final class RowCoverage {

        /** 当前行各字段所属簇。 */
        private final Map<String, String> coveredClusters =
                new LinkedHashMap<String, String>();
        /** 当前行全部字段覆盖贡献之和。 */
        private double coverageScore;

        private void add(String column,
                         String cluster,
                         double contribution) {
            if (coveredClusters.put(column, cluster) != null) {
                throw new IllegalArgumentException("采样覆盖行内字段重复：" + column);
            }
            coverageScore += contribution;
        }
    }
}
