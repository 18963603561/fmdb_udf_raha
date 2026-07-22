package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringDistanceMetric;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import smile.clustering.HierarchicalClustering;
import smile.clustering.linkage.UPGMALinkage;
import smile.math.distance.Distance;

/**
 * 基于 Smile 的层次聚类实现。
 *
 * <p>实现语义与原有平均链接余弦聚类对齐，用于替换默认的自研层次聚类路径。</p>
 */
public final class SmileHierarchicalColumnClusterer implements ColumnClusterer {

    /** Smile 层次聚类的稳定算法名。 */
    public static final String ALGORITHM = "smile_hierarchical_average_cosine_v1";

    /** 余弦距离函数。 */
    private static final Distance<double[]> COSINE_DISTANCE =
            new Distance<double[]>() {
                @Override
                public double d(double[] first, double[] second) {
                    return cosineDistance(first, second);
                }
            };

    /** 聚类版本生成器。 */
    private final ClusterVersioner versioner;
    /** 时间源。 */
    private final Clock clock;

    public SmileHierarchicalColumnClusterer(ClusterVersioner versioner, Clock clock) {
        if (versioner == null || clock == null) {
            throw new IllegalArgumentException("Smile 聚类器依赖不能为空");
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
            return singleSampleResult(columnName, dictionary, sortedRows.get(0),
                    config, randomSeed);
        }
        if (dictionary.getDefinitions().isEmpty()) {
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.EMPTY_FEATURES, "当前字段没有可计算距离的有效特征");
        }

        double[][] data = denseRows(dictionary, sortedRows);
        if (allZero(data)) {
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.EMPTY_FEATURES, "当前字段没有可计算距离的有效特征");
        }
        try {
            int target = Math.min(config.getTargetClusterCount(), data.length);
            HierarchicalClustering model = HierarchicalClustering.fit(
                    UPGMALinkage.of(data, COSINE_DISTANCE));
            int[] memberships = model.partition(target);
            return buildResult(columnName, dictionary, sortedRows, data, memberships,
                    config, randomSeed);
        } catch (RuntimeException exception) {
            return failedResult(columnName, dictionary, config, randomSeed, exception);
        }
    }

    private ColumnClusteringResult buildResult(String columnName,
                                               FeatureDictionary dictionary,
                                               List<SparseFeatureRow> rows,
                                               double[][] data,
                                               int[] memberships,
                                               ClusteringConfig config,
                                               long randomSeed) {
        Map<Integer, List<Integer>> membersByCluster =
                new LinkedHashMap<Integer, List<Integer>>();
        for (int index = 0; index < memberships.length; index++) {
            int clusterIndex = memberships[index];
            if (!membersByCluster.containsKey(clusterIndex)) {
                membersByCluster.put(clusterIndex, new ArrayList<Integer>());
            }
            membersByCluster.get(clusterIndex).add(index);
        }

        List<ClusterGroup> groups = new ArrayList<ClusterGroup>();
        for (Map.Entry<Integer, List<Integer>> entry : membersByCluster.entrySet()) {
            groups.add(new ClusterGroup(entry.getKey(), entry.getValue(),
                    signature(entry.getValue(), rows)));
        }
        Collections.sort(groups, Comparator.comparing(group -> group.signature));

        Map<Integer, String> clusterIds = new LinkedHashMap<Integer, String>();
        for (int index = 0; index < groups.size(); index++) {
            clusterIds.put(groups.get(index).clusterIndex,
                    String.format(java.util.Locale.ROOT, "cluster-%04d", index + 1));
        }

        Map<String, String> membershipsByCell = new LinkedHashMap<String, String>();
        for (int index = 0; index < rows.size(); index++) {
            membershipsByCell.put(rows.get(index).getCellId(),
                    clusterIds.get(memberships[index]));
        }

        String version = versioner.versionOf(columnName, dictionary.getVersion(), ALGORITHM,
                config, randomSeed, ColumnClusteringStatus.SUCCEEDED, membershipsByCell);
        List<ClusterAssignment> assignments =
                new ArrayList<ClusterAssignment>(rows.size());
        for (ClusterGroup group : groups) {
            double[] centroid = centroid(group.members, data);
            String clusterId = clusterIds.get(group.clusterIndex);
            for (Integer rowIndex : group.members) {
                SparseFeatureRow row = rows.get(rowIndex);
                assignments.add(new ClusterAssignment(row.getCellId(), columnName,
                        row.getCoordinate(), clusterId, ALGORITHM, version,
                        cosineDistance(data[rowIndex], centroid)));
            }
        }
        Collections.sort(assignments, Comparator.comparing(ClusterAssignment::getCellId));
        return new ColumnClusteringResult(columnName, ALGORITHM, config.getDistanceMetric(),
                config.getTargetClusterCount(), groups.size(), randomSeed, version,
                ColumnClusteringStatus.SUCCEEDED, "Smile 层次聚类完成",
                assignments, clock.millis());
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
                ColumnClusteringStatus.SINGLE_SAMPLE, "单样本直接形成独立簇",
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

    private ColumnClusteringResult failedResult(String columnName,
                                                FeatureDictionary dictionary,
                                                ClusteringConfig config,
                                                long randomSeed,
                                                RuntimeException exception) {
        String version = versioner.versionOf(columnName, dictionary.getVersion(), ALGORITHM,
                config, randomSeed, ColumnClusteringStatus.FAILED,
                Collections.<String, String>emptyMap());
        return new ColumnClusteringResult(columnName, ALGORITHM, config.getDistanceMetric(),
                config.getTargetClusterCount(), 0, randomSeed, version,
                ColumnClusteringStatus.FAILED,
                "Smile 聚类异常已隔离：" + exception.getClass().getSimpleName(),
                Collections.<ClusterAssignment>emptyList(), clock.millis());
    }

    private static void validate(String columnName,
                                 FeatureDictionary dictionary,
                                 List<SparseFeatureRow> rows,
                                 ClusteringConfig config) {
        if (columnName == null || columnName.trim().isEmpty()
                || dictionary == null || rows == null || config == null) {
            throw new IllegalArgumentException("聚类参数不能为空");
        }
        if (!columnName.equals(dictionary.getColumnName())
                || config.getDistanceMetric() != ClusteringDistanceMetric.COSINE) {
            throw new IllegalArgumentException("聚类字段或距离配置不受支持");
        }
        for (SparseFeatureRow row : rows) {
            if (row == null || !columnName.equals(row.getColumnName())
                    || !dictionary.getVersion().equals(row.getFeatureDictionaryVersion())
                    || row.getCoordinate() == null) {
                throw new IllegalArgumentException("聚类特征行与字段、字典或坐标不一致");
            }
        }
    }

    private static double[][] denseRows(FeatureDictionary dictionary,
                                        List<SparseFeatureRow> rows) {
        int dimension = featureDimension(dictionary);
        double[][] data = new double[rows.size()][dimension];
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            for (Map.Entry<Integer, Double> entry : rows.get(rowIndex).getValues().entrySet()) {
                if (entry.getKey() < 0 || entry.getKey() >= dimension) {
                    throw new IllegalArgumentException("特征索引超出字典范围");
                }
                data[rowIndex][entry.getKey()] = entry.getValue();
            }
        }
        return data;
    }

    private static int featureDimension(FeatureDictionary dictionary) {
        int maxIndex = -1;
        for (Integer index : dictionary.getDefinitions().keySet()) {
            if (index != null && index > maxIndex) {
                maxIndex = index;
            }
        }
        return maxIndex + 1;
    }

    private static boolean allZero(double[][] data) {
        for (double[] row : data) {
            for (double value : row) {
                if (value != 0.0d) {
                    return false;
                }
            }
        }
        return true;
    }

    private static double[] centroid(List<Integer> members, double[][] data) {
        double[] centroid = new double[data[0].length];
        for (Integer rowIndex : members) {
            double[] row = data[rowIndex];
            for (int index = 0; index < row.length; index++) {
                centroid[index] += row[index];
            }
        }
        for (int index = 0; index < centroid.length; index++) {
            centroid[index] /= members.size();
        }
        return centroid;
    }

    private static double cosineDistance(double[] first, double[] second) {
        double firstNorm = norm(first);
        double secondNorm = norm(second);
        if (firstNorm == 0.0d && secondNorm == 0.0d) {
            return 0.0d;
        }
        if (firstNorm == 0.0d || secondNorm == 0.0d) {
            return 1.0d;
        }
        double dot = 0.0d;
        for (int index = 0; index < first.length; index++) {
            dot += first[index] * second[index];
        }
        double similarity = dot / (firstNorm * secondNorm);
        return Math.max(0.0d, Math.min(2.0d, 1.0d - similarity));
    }

    private static double norm(double[] values) {
        double sum = 0.0d;
        for (double value : values) {
            sum += value * value;
        }
        return Math.sqrt(sum);
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

    private static final class ClusterGroup {
        /** 聚类内部编号。 */
        private final int clusterIndex;
        /** 当前簇成员行索引。 */
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
