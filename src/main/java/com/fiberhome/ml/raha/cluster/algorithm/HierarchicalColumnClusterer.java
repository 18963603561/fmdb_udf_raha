package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 使用平均连接和余弦距离实现适用于小表的精确层次聚类。
 */
public final class HierarchicalColumnClusterer implements ColumnClusterer {

    /** 稳定算法名称。 */
    public static final String ALGORITHM = "hierarchical_average_cosine_v1";
    /** 聚类版本生成器。 */
    private final ClusterVersioner versioner;
    /** 提供可测试创建时间的时钟。 */
    private final Clock clock;

    public HierarchicalColumnClusterer(ClusterVersioner versioner, Clock clock) {
        if (versioner == null || clock == null) {
            throw new IllegalArgumentException("层次聚类器依赖不能为空");
        }
        this.versioner = versioner;
        this.clock = clock;
    }

    @Override
    public String getAlgorithm() {
        return ALGORITHM;
    }

    @Override
    public ColumnClusteringResult cluster(String columnName,
                                          FeatureDictionary dictionary,
                                          List<SparseFeatureRow> rows,
                                          ClusteringConfig config,
                                          long randomSeed) {
        validate(columnName, dictionary, rows, config);
        List<SparseFeatureRow> sortedRows = new ArrayList<SparseFeatureRow>(rows);
        Collections.sort(sortedRows, Comparator.comparing(SparseFeatureRow::getCellId));
        if (sortedRows.isEmpty()) {
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.EMPTY_INPUT, "当前字段没有单元格特征");
        }
        if (sortedRows.size() > config.getMaxSampleCount()) {
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.INPUT_LIMIT_EXCEEDED, "当前字段样本数量超过精确聚类上限");
        }
        if (sortedRows.size() == 1) {
            return singleSampleResult(columnName, dictionary, sortedRows.get(0), config, randomSeed);
        }
        if (dictionary.getDefinitions().isEmpty() || allZero(sortedRows)) {
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.EMPTY_FEATURES, "当前字段没有可计算距离的有效特征");
        }
        return clusterRows(columnName, dictionary, sortedRows, config, randomSeed);
    }

    private ColumnClusteringResult clusterRows(String columnName,
                                               FeatureDictionary dictionary,
                                               List<SparseFeatureRow> rows,
                                               ClusteringConfig config,
                                               long randomSeed) {
        int target = Math.min(config.getTargetClusterCount(), rows.size());
        List<WorkingCluster> clusters = new ArrayList<WorkingCluster>();
        for (int index = 0; index < rows.size(); index++) {
            clusters.add(new WorkingCluster(index));
        }
        while (clusters.size() > target) {
            MergeCandidate best = null;
            for (int left = 0; left < clusters.size(); left++) {
                for (int right = left + 1; right < clusters.size(); right++) {
                    double distance = averageDistance(
                            clusters.get(left), clusters.get(right), rows);
                    if (Double.isNaN(distance) || Double.isInfinite(distance)) {
                        return emptyResult(columnName, dictionary, config, randomSeed,
                                ColumnClusteringStatus.DISTANCE_UNDEFINED,
                                "当前字段特征距离无法计算");
                    }
                    MergeCandidate candidate = new MergeCandidate(left, right, distance,
                            tieKey(clusters.get(left), clusters.get(right), rows, randomSeed));
                    if (best == null || candidate.compareTo(best) < 0) {
                        best = candidate;
                    }
                }
            }
            // 多样本且目标簇数量更小时必须存在可合并簇，否则返回可解释失败状态。
            if (best == null) {
                return emptyResult(columnName, dictionary, config, randomSeed,
                        ColumnClusteringStatus.FAILED, "未找到可合并的聚类成员");
            }
            WorkingCluster merged = clusters.get(best.left).merge(clusters.get(best.right));
            clusters.remove(best.right);
            clusters.remove(best.left);
            clusters.add(merged);
        }
        Collections.sort(clusters, Comparator.comparing(cluster -> cluster.signature(rows)));
        Map<String, String> memberships = new LinkedHashMap<String, String>();
        for (int clusterIndex = 0; clusterIndex < clusters.size(); clusterIndex++) {
            String clusterId = String.format(java.util.Locale.ROOT,
                    "cluster-%04d", clusterIndex + 1);
            for (Integer rowIndex : clusters.get(clusterIndex).members) {
                memberships.put(rows.get(rowIndex).getCellId(), clusterId);
            }
        }
        String version = versioner.versionOf(columnName, dictionary.getVersion(),
                ALGORITHM, config, randomSeed, ColumnClusteringStatus.SUCCEEDED, memberships);
        List<ClusterAssignment> assignments = new ArrayList<ClusterAssignment>(rows.size());
        for (WorkingCluster cluster : clusters) {
            Map<Integer, Double> centroid = centroid(cluster, rows);
            for (Integer rowIndex : cluster.members) {
                SparseFeatureRow row = rows.get(rowIndex);
                assignments.add(new ClusterAssignment(row.getCellId(), columnName,
                        row.getCoordinate(), memberships.get(row.getCellId()), ALGORITHM,
                        version, cosineDistance(row.getValues(), centroid)));
            }
        }
        Collections.sort(assignments, Comparator.comparing(ClusterAssignment::getCellId));
        return new ColumnClusteringResult(columnName, ALGORITHM, config.getDistanceMetric(),
                config.getTargetClusterCount(), clusters.size(), randomSeed, version,
                ColumnClusteringStatus.SUCCEEDED, "聚类完成", assignments, clock.millis());
    }

    private ColumnClusteringResult singleSampleResult(String columnName,
                                                       FeatureDictionary dictionary,
                                                       SparseFeatureRow row,
                                                       ClusteringConfig config,
                                                       long randomSeed) {
        Map<String, String> memberships = Collections.singletonMap(
                row.getCellId(), "cluster-0001");
        String version = versioner.versionOf(columnName, dictionary.getVersion(), ALGORITHM,
                config, randomSeed, ColumnClusteringStatus.SINGLE_SAMPLE, memberships);
        ClusterAssignment assignment = new ClusterAssignment(row.getCellId(), columnName,
                row.getCoordinate(), "cluster-0001", ALGORITHM, version, 0.0d);
        return new ColumnClusteringResult(columnName, ALGORITHM, config.getDistanceMetric(),
                config.getTargetClusterCount(), 1, randomSeed, version,
                ColumnClusteringStatus.SINGLE_SAMPLE, "单样本直接形成独立聚类",
                Collections.singletonList(assignment), clock.millis());
    }

    private ColumnClusteringResult emptyResult(String columnName,
                                                FeatureDictionary dictionary,
                                                ClusteringConfig config,
                                                long randomSeed,
                                                ColumnClusteringStatus status,
                                                String message) {
        String version = versioner.versionOf(columnName, dictionary.getVersion(), ALGORITHM,
                config, randomSeed, status, Collections.<String, String>emptyMap());
        return new ColumnClusteringResult(columnName, ALGORITHM, config.getDistanceMetric(),
                config.getTargetClusterCount(), 0, randomSeed, version, status, message,
                Collections.<ClusterAssignment>emptyList(), clock.millis());
    }

    private static void validate(String columnName,
                                 FeatureDictionary dictionary,
                                 List<SparseFeatureRow> rows,
                                 ClusteringConfig config) {
        if (columnName == null || columnName.trim().isEmpty()
                || dictionary == null || rows == null || config == null) {
            throw new IllegalArgumentException("列聚类参数不能为空");
        }
        if (!columnName.equals(dictionary.getColumnName())
                || config.getDistanceMetric() != ClusteringDistanceMetric.COSINE) {
            throw new IllegalArgumentException("列聚类字段或距离配置不受支持");
        }
        for (SparseFeatureRow row : rows) {
            if (row == null || !columnName.equals(row.getColumnName())
                    || !dictionary.getVersion().equals(row.getFeatureDictionaryVersion())
                    || row.getCoordinate() == null) {
                throw new IllegalArgumentException("聚类特征行与字段、字典或坐标不一致");
            }
        }
    }

    private static boolean allZero(List<SparseFeatureRow> rows) {
        for (SparseFeatureRow row : rows) {
            if (!row.getValues().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static double averageDistance(WorkingCluster left,
                                          WorkingCluster right,
                                          List<SparseFeatureRow> rows) {
        double sum = 0.0d;
        int count = 0;
        for (Integer leftIndex : left.members) {
            for (Integer rightIndex : right.members) {
                sum += cosineDistance(rows.get(leftIndex).getValues(),
                        rows.get(rightIndex).getValues());
                count++;
            }
        }
        return count == 0 ? Double.NaN : sum / count;
    }

    private static double cosineDistance(Map<Integer, Double> first,
                                         Map<Integer, Double> second) {
        double firstNorm = norm(first);
        double secondNorm = norm(second);
        if (firstNorm == 0.0d && secondNorm == 0.0d) {
            return 0.0d;
        }
        if (firstNorm == 0.0d || secondNorm == 0.0d) {
            return 1.0d;
        }
        Map<Integer, Double> smaller = first.size() <= second.size() ? first : second;
        Map<Integer, Double> larger = first.size() <= second.size() ? second : first;
        double dot = 0.0d;
        for (Map.Entry<Integer, Double> entry : smaller.entrySet()) {
            Double value = larger.get(entry.getKey());
            if (value != null) {
                dot += entry.getValue() * value;
            }
        }
        double similarity = dot / (firstNorm * secondNorm);
        return Math.max(0.0d, Math.min(2.0d, 1.0d - similarity));
    }

    private static double norm(Map<Integer, Double> values) {
        double sum = 0.0d;
        for (Double value : values.values()) {
            sum += value * value;
        }
        return Math.sqrt(sum);
    }

    private static Map<Integer, Double> centroid(WorkingCluster cluster,
                                                 List<SparseFeatureRow> rows) {
        Map<Integer, Double> centroid = new LinkedHashMap<Integer, Double>();
        for (Integer rowIndex : cluster.members) {
            for (Map.Entry<Integer, Double> entry : rows.get(rowIndex).getValues().entrySet()) {
                Double value = centroid.get(entry.getKey());
                centroid.put(entry.getKey(), (value == null ? 0.0d : value) + entry.getValue());
            }
        }
        for (Map.Entry<Integer, Double> entry
                : new ArrayList<Map.Entry<Integer, Double>>(centroid.entrySet())) {
            centroid.put(entry.getKey(), entry.getValue() / cluster.members.size());
        }
        return centroid;
    }

    private static String tieKey(WorkingCluster left,
                                 WorkingCluster right,
                                 List<SparseFeatureRow> rows,
                                 long randomSeed) {
        String first = left.signature(rows);
        String second = right.signature(rows);
        return HashUtils.md5Hex(randomSeed + "|" + first + "|" + second);
    }

    private static final class WorkingCluster {
        /** 当前簇包含的特征行序号。 */
        private final List<Integer> members;

        private WorkingCluster(int member) {
            this.members = new ArrayList<Integer>();
            this.members.add(member);
        }

        private WorkingCluster(List<Integer> members) {
            this.members = members;
        }

        private WorkingCluster merge(WorkingCluster other) {
            List<Integer> merged = new ArrayList<Integer>(members);
            merged.addAll(other.members);
            Collections.sort(merged);
            return new WorkingCluster(merged);
        }

        private String signature(List<SparseFeatureRow> rows) {
            List<String> cellIds = new ArrayList<String>();
            for (Integer member : members) {
                cellIds.add(rows.get(member).getCellId());
            }
            Collections.sort(cellIds);
            return String.join("|", cellIds);
        }
    }

    private static final class MergeCandidate implements Comparable<MergeCandidate> {
        /** 左侧簇序号。 */
        private final int left;
        /** 右侧簇序号。 */
        private final int right;
        /** 平均连接距离。 */
        private final double distance;
        /** 使用随机种子生成的稳定并列排序键。 */
        private final String tieKey;

        private MergeCandidate(int left, int right, double distance, String tieKey) {
            this.left = left;
            this.right = right;
            this.distance = distance;
            this.tieKey = tieKey;
        }

        @Override
        public int compareTo(MergeCandidate other) {
            int distanceCompare = Double.compare(distance, other.distance);
            if (distanceCompare != 0) {
                return distanceCompare;
            }
            int tieCompare = tieKey.compareTo(other.tieKey);
            if (tieCompare != 0) {
                return tieCompare;
            }
            int leftCompare = Integer.compare(left, other.left);
            return leftCompare == 0 ? Integer.compare(right, other.right) : leftCompare;
        }
    }
}
