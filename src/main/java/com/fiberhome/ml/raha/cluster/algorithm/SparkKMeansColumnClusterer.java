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
import java.util.TreeMap;
import org.apache.spark.SparkEnv;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 Spark MLlib KMeans 的大样本列聚类实现。
 *
 * <p>该实现只在 Spark driver 侧发起训练和预测，输入仍沿用当前工程的单列稀疏特征行，
 * 输出保持现有 `ColumnClusteringResult` 与 `ClusterAssignment` 语义。</p>
 */
public final class SparkKMeansColumnClusterer implements ColumnClusterer {

    /** Spark KMeans 聚类算法稳定名称。 */
    public static final String ALGORITHM = "spark_kmeans_cosine_v1";
    /** Spark KMeans 最大迭代次数，第一阶段先固定在代码中。 */
    private static final int MAX_ITERATIONS = 50;
    /** Spark KMeans 初始化步数，第一阶段先固定在代码中。 */
    private static final int INIT_STEPS = 2;
    /** 单元格标识列名。 */
    private static final String CELL_ID_COL = "cell_id";
    /** 排序后行序号列名。 */
    private static final String ROW_ORDER_COL = "row_order";
    /** Spark MLlib 特征向量列名。 */
    private static final String FEATURES_COL = "features";
    /** Spark MLlib 预测簇编号列名。 */
    private static final String PREDICTION_COL = "prediction";
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SparkKMeansColumnClusterer.class);

    /** driver 侧 Spark 会话。 */
    private final SparkSession sparkSession;
    /** 聚类版本生成器。 */
    private final ClusterVersioner versioner;
    /** 时间源。 */
    private final Clock clock;

    public SparkKMeansColumnClusterer(SparkSession sparkSession,
                                      ClusterVersioner versioner,
                                      Clock clock) {
        if (sparkSession == null || versioner == null || clock == null) {
            throw new IllegalArgumentException("Spark KMeans 聚类器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.versioner = versioner;
        this.clock = clock;
    }

    @Override
    public String getAlgorithm() {
        return ALGORITHM;
    }

    @Override
    public boolean supportsLocalColumnParallelism() {
        return false;
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
            LOGGER.warn("Spark KMeans 聚类输入为空，columnName={}", columnName);
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.EMPTY_INPUT, "当前字段没有单元格特征");
        }
        if (sortedRows.size() == 1) {
            LOGGER.warn("Spark KMeans 聚类遇到单样本，columnName={}", columnName);
            return singleSampleResult(columnName, dictionary, sortedRows.get(0),
                    config, randomSeed);
        }
        if (dictionary.getDefinitions().isEmpty()) {
            LOGGER.warn("Spark KMeans 聚类缺少有效特征字典，columnName={}", columnName);
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.EMPTY_FEATURES, "当前字段没有可计算距离的有效特征");
        }
        int dimension = featureDimension(dictionary);
        if (dimension <= 0 || allZero(sortedRows)) {
            LOGGER.warn("Spark KMeans 聚类特征全零，columnName={}，rowCount={}",
                    columnName, sortedRows.size());
            return emptyResult(columnName, dictionary, config, randomSeed,
                    ColumnClusteringStatus.EMPTY_FEATURES, "当前字段没有可计算距离的有效特征");
        }
        if (!isDriverSide()) {
            LOGGER.error("Spark KMeans 聚类必须在 driver 侧发起，columnName={}", columnName);
            return failedResult(columnName, dictionary, config, randomSeed,
                    new IllegalStateException("Spark KMeans 只能在 driver 侧发起"));
        }
        long startedAt = clock.millis();
        LOGGER.info("开始 Spark KMeans 列聚类，columnName={}，rowCount={}，targetClusterCount={}，randomSeed={}",
                columnName, sortedRows.size(), config.getTargetClusterCount(), randomSeed);
        try {
            Dataset<Row> frame = featureFrame(sortedRows, dimension);
            int target = Math.min(config.getTargetClusterCount(), sortedRows.size());
            KMeansModel model = new KMeans()
                    .setFeaturesCol(FEATURES_COL)
                    .setPredictionCol(PREDICTION_COL)
                    .setK(target)
                    .setSeed(randomSeed)
                    .setDistanceMeasure("cosine")
                    .setMaxIter(MAX_ITERATIONS)
                    .setInitMode("k-means||")
                    .setInitSteps(INIT_STEPS)
                    .fit(frame);
            List<Row> predictionRows = model.transform(frame)
                    .select(CELL_ID_COL, ROW_ORDER_COL, PREDICTION_COL)
                    .collectAsList();
            ColumnClusteringResult result = buildResult(columnName, dictionary,
                    sortedRows, predictionRows, model.clusterCenters(), config, randomSeed);
            LOGGER.info("Spark KMeans 列聚类完成，columnName={}，effectiveClusterCount={}，assignmentCount={}，costMillis={}",
                    columnName, result.getEffectiveClusterCount(),
                    result.getAssignments().size(), clock.millis() - startedAt);
            return result;
        } catch (RuntimeException | LinkageError exception) {
            // Spark MLlib 外部依赖异常需要完整堆栈，便于定位运行环境和算法输入问题。
            LOGGER.error("Spark KMeans 列聚类失败，columnName={}，rowCount={}",
                    columnName, sortedRows.size(), exception);
            return failedResult(columnName, dictionary, config, randomSeed, exception);
        }
    }

    private ColumnClusteringResult buildResult(String columnName,
                                               FeatureDictionary dictionary,
                                               List<SparseFeatureRow> rows,
                                               List<Row> predictionRows,
                                               Vector[] centers,
                                               ClusteringConfig config,
                                               long randomSeed) {
        Map<String, Integer> predictionByCell = new LinkedHashMap<String, Integer>();
        Map<Integer, List<Integer>> membersByPrediction =
                new LinkedHashMap<Integer, List<Integer>>();
        for (Row row : predictionRows) {
            String cellId = row.getAs(CELL_ID_COL);
            Number rowOrder = row.getAs(ROW_ORDER_COL);
            Number prediction = row.getAs(PREDICTION_COL);
            int rowIndex = rowOrder.intValue();
            int predictionIndex = prediction.intValue();
            predictionByCell.put(cellId, predictionIndex);
            if (!membersByPrediction.containsKey(predictionIndex)) {
                membersByPrediction.put(predictionIndex, new ArrayList<Integer>());
            }
            membersByPrediction.get(predictionIndex).add(rowIndex);
        }
        if (predictionByCell.size() != rows.size()) {
            throw new IllegalStateException("Spark KMeans 预测结果数量与输入样本数量不一致");
        }
        List<ClusterGroup> groups = new ArrayList<ClusterGroup>();
        for (Map.Entry<Integer, List<Integer>> entry : membersByPrediction.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                groups.add(new ClusterGroup(entry.getKey(), entry.getValue(),
                        signature(entry.getValue(), rows)));
            }
        }
        Collections.sort(groups, Comparator.comparing(group -> group.signature));

        Map<Integer, String> clusterIds = new LinkedHashMap<Integer, String>();
        for (int index = 0; index < groups.size(); index++) {
            clusterIds.put(groups.get(index).prediction,
                    String.format(java.util.Locale.ROOT, "cluster-%04d", index + 1));
        }

        Map<String, String> memberships = new LinkedHashMap<String, String>();
        for (SparseFeatureRow row : rows) {
            Integer prediction = predictionByCell.get(row.getCellId());
            memberships.put(row.getCellId(), clusterIds.get(prediction));
        }
        String clusterVersion = versioner.versionOf(columnName, dictionary.getVersion(),
                ALGORITHM, config, randomSeed, ColumnClusteringStatus.SUCCEEDED,
                memberships);
        List<ClusterAssignment> assignments =
                new ArrayList<ClusterAssignment>(rows.size());
        for (SparseFeatureRow row : rows) {
            Integer prediction = predictionByCell.get(row.getCellId());
            assignments.add(new ClusterAssignment(row.getCellId(), columnName,
                    row.getCoordinate(), memberships.get(row.getCellId()), ALGORITHM,
                    clusterVersion,
                    cosineDistance(row.getValues(), centers[prediction].toArray())));
        }
        Collections.sort(assignments, Comparator.comparing(ClusterAssignment::getCellId));
        return new ColumnClusteringResult(columnName, ALGORITHM,
                config.getDistanceMetric(), config.getTargetClusterCount(),
                groups.size(), randomSeed, clusterVersion,
                ColumnClusteringStatus.SUCCEEDED, "Spark KMeans 聚类完成",
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
                                                Throwable exception) {
        String version = versioner.versionOf(columnName, dictionary.getVersion(), ALGORITHM,
                config, randomSeed, ColumnClusteringStatus.FAILED,
                Collections.<String, String>emptyMap());
        return new ColumnClusteringResult(columnName, ALGORITHM, config.getDistanceMetric(),
                config.getTargetClusterCount(), 0, randomSeed, version,
                ColumnClusteringStatus.FAILED,
                "Spark KMeans 聚类异常已隔离：" + exception.getClass().getSimpleName(),
                Collections.<ClusterAssignment>emptyList(), clock.millis());
    }

    private Dataset<Row> featureFrame(List<SparseFeatureRow> rows, int dimension) {
        List<Row> frameRows = new ArrayList<Row>(rows.size());
        for (int index = 0; index < rows.size(); index++) {
            frameRows.add(RowFactory.create(rows.get(index).getCellId(),
                    Integer.valueOf(index), vector(dimension, rows.get(index).getValues())));
        }
        StructType schema = new StructType(new StructField[]{
                new StructField(CELL_ID_COL, DataTypes.StringType, false, Metadata.empty()),
                new StructField(ROW_ORDER_COL, DataTypes.IntegerType, false, Metadata.empty()),
                new StructField(FEATURES_COL, new VectorUDT(), false, Metadata.empty())
        });
        return sparkSession.createDataFrame(frameRows, schema);
    }

    private static Vector vector(int dimension, Map<Integer, Double> values) {
        TreeMap<Integer, Double> sorted = new TreeMap<Integer, Double>(values);
        int[] indices = new int[sorted.size()];
        double[] nonzeroValues = new double[sorted.size()];
        int index = 0;
        for (Map.Entry<Integer, Double> entry : sorted.entrySet()) {
            if (entry.getKey() < 0 || entry.getKey() >= dimension) {
                throw new IllegalArgumentException("特征索引超出字典范围");
            }
            indices[index] = entry.getKey();
            nonzeroValues[index] = entry.getValue();
            index++;
        }
        return Vectors.sparse(dimension, indices, nonzeroValues);
    }

    private static void validate(String columnName,
                                 FeatureDictionary dictionary,
                                 List<SparseFeatureRow> rows,
                                 ClusteringConfig config) {
        if (columnName == null || columnName.trim().isEmpty()
                || dictionary == null || rows == null || config == null) {
            throw new IllegalArgumentException("Spark KMeans 聚类参数不能为空");
        }
        if (!columnName.equals(dictionary.getColumnName())
                || config.getDistanceMetric() != ClusteringDistanceMetric.COSINE) {
            throw new IllegalArgumentException("Spark KMeans 聚类字段或距离配置不受支持");
        }
        for (SparseFeatureRow row : rows) {
            if (row == null || !columnName.equals(row.getColumnName())
                    || !dictionary.getVersion().equals(row.getFeatureDictionaryVersion())
                    || row.getCoordinate() == null) {
                throw new IllegalArgumentException("Spark KMeans 特征行与字段、字典或坐标不一致");
            }
        }
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

    private static boolean allZero(List<SparseFeatureRow> rows) {
        for (SparseFeatureRow row : rows) {
            for (Double value : row.getValues().values()) {
                if (value != null && Double.compare(value, 0.0d) != 0) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean isDriverSide() {
        SparkEnv env = SparkEnv.get();
        return env == null || "driver".equals(env.executorId());
    }

    private static double cosineDistance(Map<Integer, Double> first,
                                         double[] second) {
        double firstNorm = norm(first);
        double secondNorm = norm(second);
        if (firstNorm == 0.0d && secondNorm == 0.0d) {
            return 0.0d;
        }
        if (firstNorm == 0.0d || secondNorm == 0.0d) {
            return 1.0d;
        }
        double dot = 0.0d;
        for (Map.Entry<Integer, Double> entry : first.entrySet()) {
            int index = entry.getKey();
            if (index >= 0 && index < second.length) {
                dot += entry.getValue() * second[index];
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
        /** Spark 预测簇编号。 */
        private final int prediction;
        /** 当前簇成员在排序后输入中的行序号。 */
        private final List<Integer> members;
        /** 用于稳定排序的成员签名。 */
        private final String signature;

        private ClusterGroup(int prediction,
                             List<Integer> members,
                             String signature) {
            this.prediction = prediction;
            this.members = members;
            this.signature = signature;
        }
    }
}
