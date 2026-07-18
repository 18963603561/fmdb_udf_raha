package com.fiberhome.ml.raha.cluster;

import com.fiberhome.ml.raha.config.ClusteringConfig;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.util.HashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 小数据使用精确层次聚类，大数据自动切换到确定性余弦分区聚类。
 */
public final class ScalableColumnClusterer implements ColumnClusterer {

    /** 大样本确定性聚类算法名称。 */
    public static final String APPROXIMATE_ALGORITHM =
            "deterministic_cosine_partition_v1";
    /** 大样本聚类最大迭代次数。 */
    private static final int MAX_ITERATIONS = 100;
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ScalableColumnClusterer.class);
    /** 小样本精确层次聚类器。 */
    private final HierarchicalColumnClusterer exactClusterer;
    /** 聚类版本生成器。 */
    private final ClusterVersioner versioner;
    /** 提供可测试创建时间的时钟。 */
    private final Clock clock;

    public ScalableColumnClusterer(ClusterVersioner versioner, Clock clock) {
        if (versioner == null || clock == null) {
            throw new IllegalArgumentException("可扩展聚类器依赖不能为空");
        }
        this.versioner = versioner;
        this.clock = clock;
        this.exactClusterer = new HierarchicalColumnClusterer(versioner, clock);
    }

    @Override
    public String getAlgorithm() {
        return APPROXIMATE_ALGORITHM;
    }

    @Override
    public ColumnClusteringResult cluster(String columnName,
                                          FeatureDictionary dictionary,
                                          List<SparseFeatureRow> rows,
                                          ClusteringConfig config,
                                          long randomSeed) {
        validate(columnName, dictionary, rows, config);
        if (rows.size() <= config.getMaxSampleCount()) {
            return exactClusterer.cluster(columnName, dictionary, rows,
                    config, randomSeed);
        }
        LOGGER.info("字段样本超过精确聚类上限，切换确定性余弦聚类，columnName={}，"
                        + "rowCount={}，exactLimit={}，targetClusterCount={}",
                columnName, rows.size(), config.getMaxSampleCount(),
                config.getTargetClusterCount());
        return partition(columnName, dictionary, rows, config, randomSeed);
    }

    private ColumnClusteringResult partition(String columnName,
                                              FeatureDictionary dictionary,
                                              List<SparseFeatureRow> inputRows,
                                              ClusteringConfig config,
                                              long randomSeed) {
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>(inputRows);
        Collections.sort(rows, Comparator.comparing(SparseFeatureRow::getCellId));
        if (rows.isEmpty()) {
            return exactClusterer.cluster(columnName, dictionary, rows,
                    config, randomSeed);
        }
        int target = Math.min(config.getTargetClusterCount(), rows.size());
        List<Map<Integer, Double>> centroids = initialCentroids(rows, target, randomSeed);
        int[] assignments = new int[rows.size()];
        java.util.Arrays.fill(assignments, -1);
        for (int iteration = 0; iteration < MAX_ITERATIONS; iteration++) {
            int[] updated = assign(rows, centroids, randomSeed);
            if (java.util.Arrays.equals(assignments, updated)) {
                assignments = updated;
                break;
            }
            assignments = updated;
            centroids = centroids(rows, assignments, centroids.size(), centroids);
        }
        Map<Integer, List<Integer>> members = members(assignments, target);
        List<ClusterGroup> groups = new ArrayList<ClusterGroup>();
        for (Map.Entry<Integer, List<Integer>> entry : members.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                groups.add(new ClusterGroup(entry.getKey(), entry.getValue(),
                        signature(entry.getValue(), rows)));
            }
        }
        Collections.sort(groups, Comparator.comparing(group -> group.signature));
        Map<Integer, String> clusterIds = new LinkedHashMap<Integer, String>();
        for (int index = 0; index < groups.size(); index++) {
            clusterIds.put(groups.get(index).clusterIndex,
                    String.format(java.util.Locale.ROOT, "cluster-%04d", index + 1));
        }
        Map<String, String> memberships = new LinkedHashMap<String, String>();
        for (int index = 0; index < rows.size(); index++) {
            memberships.put(rows.get(index).getCellId(), clusterIds.get(assignments[index]));
        }
        String clusterVersion = versioner.versionOf(columnName, dictionary.getVersion(),
                APPROXIMATE_ALGORITHM, config, randomSeed,
                ColumnClusteringStatus.SUCCEEDED, memberships);
        List<ClusterAssignment> results = new ArrayList<ClusterAssignment>(rows.size());
        for (int index = 0; index < rows.size(); index++) {
            SparseFeatureRow row = rows.get(index);
            results.add(new ClusterAssignment(row.getCellId(), columnName,
                    row.getCoordinate(), memberships.get(row.getCellId()),
                    APPROXIMATE_ALGORITHM, clusterVersion,
                    cosineDistance(row.getValues(), centroids.get(assignments[index]))));
        }
        Collections.sort(results, Comparator.comparing(ClusterAssignment::getCellId));
        return new ColumnClusteringResult(columnName, APPROXIMATE_ALGORITHM,
                config.getDistanceMetric(), config.getTargetClusterCount(),
                groups.size(), randomSeed, clusterVersion,
                ColumnClusteringStatus.SUCCEEDED, "确定性余弦聚类完成",
                results, clock.millis());
    }

    private static List<Map<Integer, Double>> initialCentroids(
            List<SparseFeatureRow> rows,
            int target,
            long randomSeed) {
        List<SparseFeatureRow> ordered = new ArrayList<SparseFeatureRow>(rows);
        Collections.sort(ordered, Comparator.comparing(row ->
                HashUtils.sha256Hex(randomSeed + "|" + row.getCellId())));
        List<Map<Integer, Double>> centroids =
                new ArrayList<Map<Integer, Double>>(target);
        for (int index = 0; index < target; index++) {
            centroids.add(new LinkedHashMap<Integer, Double>(
                    ordered.get(index).getValues()));
        }
        return centroids;
    }

    private static int[] assign(List<SparseFeatureRow> rows,
                                List<Map<Integer, Double>> centroids,
                                long randomSeed) {
        int[] assignments = new int[rows.size()];
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            SparseFeatureRow row = rows.get(rowIndex);
            int bestCluster = 0;
            double bestDistance = Double.POSITIVE_INFINITY;
            String bestTie = null;
            for (int clusterIndex = 0; clusterIndex < centroids.size(); clusterIndex++) {
                double distance = cosineDistance(row.getValues(),
                        centroids.get(clusterIndex));
                String tie = HashUtils.sha256Hex(randomSeed + "|"
                        + row.getCellId() + "|" + clusterIndex);
                if (distance < bestDistance
                        || (Double.compare(distance, bestDistance) == 0
                        && (bestTie == null || tie.compareTo(bestTie) < 0))) {
                    bestDistance = distance;
                    bestCluster = clusterIndex;
                    bestTie = tie;
                }
            }
            assignments[rowIndex] = bestCluster;
        }
        return assignments;
    }

    private static List<Map<Integer, Double>> centroids(
            List<SparseFeatureRow> rows,
            int[] assignments,
            int clusterCount,
            List<Map<Integer, Double>> previous) {
        List<Map<Integer, Double>> sums = new ArrayList<Map<Integer, Double>>(clusterCount);
        int[] counts = new int[clusterCount];
        for (int index = 0; index < clusterCount; index++) {
            sums.add(new LinkedHashMap<Integer, Double>());
        }
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            int clusterIndex = assignments[rowIndex];
            counts[clusterIndex]++;
            Map<Integer, Double> sum = sums.get(clusterIndex);
            for (Map.Entry<Integer, Double> entry
                    : rows.get(rowIndex).getValues().entrySet()) {
                Double current = sum.get(entry.getKey());
                sum.put(entry.getKey(), (current == null ? 0.0d : current)
                        + entry.getValue());
            }
        }
        List<Map<Integer, Double>> values =
                new ArrayList<Map<Integer, Double>>(clusterCount);
        for (int clusterIndex = 0; clusterIndex < clusterCount; clusterIndex++) {
            if (counts[clusterIndex] == 0) {
                values.add(previous.get(clusterIndex));
                continue;
            }
            Map<Integer, Double> centroid = sums.get(clusterIndex);
            for (Map.Entry<Integer, Double> entry
                    : new ArrayList<Map.Entry<Integer, Double>>(centroid.entrySet())) {
                centroid.put(entry.getKey(), entry.getValue() / counts[clusterIndex]);
            }
            values.add(centroid);
        }
        return values;
    }

    private static Map<Integer, List<Integer>> members(int[] assignments,
                                                        int clusterCount) {
        Map<Integer, List<Integer>> members =
                new LinkedHashMap<Integer, List<Integer>>();
        for (int index = 0; index < clusterCount; index++) {
            members.put(index, new ArrayList<Integer>());
        }
        for (int rowIndex = 0; rowIndex < assignments.length; rowIndex++) {
            members.get(assignments[rowIndex]).add(rowIndex);
        }
        return members;
    }

    private static String signature(List<Integer> members,
                                    List<SparseFeatureRow> rows) {
        List<String> cellIds = new ArrayList<String>(members.size());
        for (Integer member : members) {
            cellIds.add(rows.get(member).getCellId());
        }
        Collections.sort(cellIds);
        return String.join("|", cellIds);
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

    private static void validate(String columnName,
                                 FeatureDictionary dictionary,
                                 List<SparseFeatureRow> rows,
                                 ClusteringConfig config) {
        if (columnName == null || columnName.trim().isEmpty()
                || dictionary == null || rows == null || config == null
                || !columnName.equals(dictionary.getColumnName())
                || config.getDistanceMetric() != ClusteringDistanceMetric.COSINE) {
            throw new IllegalArgumentException("可扩展聚类字段、字典和配置必须有效");
        }
        for (SparseFeatureRow row : rows) {
            if (row == null || row.getCoordinate() == null
                    || !columnName.equals(row.getColumnName())
                    || !dictionary.getVersion().equals(
                    row.getFeatureDictionaryVersion())) {
                throw new IllegalArgumentException("可扩展聚类特征行与字段或字典不一致");
            }
        }
    }

    private static final class ClusterGroup {
        /** 聚类内部序号。 */
        private final int clusterIndex;
        /** 当前簇成员行序号。 */
        private final List<Integer> members;
        /** 用于稳定排序的成员摘要。 */
        private final String signature;

        private ClusterGroup(int clusterIndex,
                             List<Integer> members,
                             String signature) {
            this.clusterIndex = clusterIndex;
            this.members = members;
            this.signature = signature;
        }
    }
}
