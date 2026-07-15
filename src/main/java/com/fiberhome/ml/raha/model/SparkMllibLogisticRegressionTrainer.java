package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ClassifierType;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 使用 Spark MLlib 权重列训练单列二分类逻辑回归，并提取可移植系数。
 */
public final class SparkMllibLogisticRegressionTrainer implements ColumnModelTrainer {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SparkMllibLogisticRegressionTrainer.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** 模型版本生成器。 */
    private final ColumnModelVersioner versioner;

    public SparkMllibLogisticRegressionTrainer(SparkSession sparkSession,
                                               ColumnModelVersioner versioner) {
        if (sparkSession == null || versioner == null) {
            throw new IllegalArgumentException("MLlib 训练器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.versioner = versioner;
    }

    @Override
    public ColumnModelTrainingResult train(ColumnModelTrainingRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("列级模型训练请求不能为空");
        }
        if (request.getDataset().getStatus() != ColumnTrainingStatus.TRAINABLE) {
            return ColumnModelTrainingResult.untrainable(request.getDataset());
        }
        LOGGER.info("开始 MLlib 列级逻辑回归训练，datasetId={}，columnName={}，sampleCount={}",
                request.getDatasetId(), request.getDataset().getColumnName(),
                request.getDataset().getExamples().size());
        try {
            Dataset<Row> trainingFrame = trainingFrame(request.getDataset());
            LogisticRegressionTrainingConfig config = request.getTrainingConfig();
            LogisticRegressionModel model = new LogisticRegression()
                    .setLabelCol("label")
                    .setFeaturesCol("features")
                    .setWeightCol("weight")
                    .setPredictionCol("prediction")
                    .setProbabilityCol("probability")
                    .setRawPredictionCol("rawPrediction")
                    .setFamily("binomial")
                    .setMaxIter(config.getMaxIterations())
                    .setRegParam(config.getRegularization())
                    .setElasticNetParam(config.getElasticNet())
                    .fit(trainingFrame);
            Map<Integer, Double> coefficients = coefficients(model.coefficients().toArray());
            String mode = "spark_mllib_logistic_regression";
            String modelVersion = versioner.versionOf(request,
                    ClassifierType.LOGISTIC_REGRESSION, model.intercept(), coefficients, mode);
            ColumnModelArtifact artifact = new ColumnModelArtifact(request.getModelName(),
                    modelVersion, request.getDataset().getColumnName(),
                    ClassifierType.LOGISTIC_REGRESSION,
                    request.getDataset().getFeatureDictionaryVersion(),
                    request.getDataset().getFeatureDimension(),
                    request.getModelConfig().getThreshold(), model.intercept(),
                    coefficients, mode);
            Map<String, Double> metrics = trainingMetrics(request.getDataset());
            LOGGER.info("MLlib 列级逻辑回归训练完成，datasetId={}，columnName={}，modelVersion={}",
                    request.getDatasetId(), request.getDataset().getColumnName(), modelVersion);
            return new ColumnModelTrainingResult(ColumnModelTrainingStatus.TRAINED,
                    artifact, false, "MLlib 逻辑回归训练完成", metrics);
        } catch (RuntimeException | LinkageError exception) {
            // MLlib 训练异常保留完整上下文和堆栈，由自适应训练器决定是否降级。
            LOGGER.error("MLlib 列级逻辑回归训练失败，datasetId={}，columnName={}",
                    request.getDatasetId(), request.getDataset().getColumnName(), exception);
            return new ColumnModelTrainingResult(ColumnModelTrainingStatus.FAILED,
                    null, false, "MLlib 逻辑回归训练失败："
                    + exception.getClass().getSimpleName(), null);
        }
    }

    private Dataset<Row> trainingFrame(ColumnTrainingDataset dataset) {
        List<Row> rows = new ArrayList<Row>(dataset.getExamples().size());
        for (ColumnTrainingExample example : dataset.getExamples()) {
            rows.add(RowFactory.create((double) example.getLabel(), example.getSampleWeight(),
                    vector(dataset.getFeatureDimension(), example.getFeatures())));
        }
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("weight", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        return sparkSession.createDataFrame(rows, schema);
    }

    private static Vector vector(int dimension, Map<Integer, Double> values) {
        TreeMap<Integer, Double> sorted = new TreeMap<Integer, Double>(values);
        int[] indices = new int[sorted.size()];
        double[] nonzeroValues = new double[sorted.size()];
        int index = 0;
        for (Map.Entry<Integer, Double> entry : sorted.entrySet()) {
            indices[index] = entry.getKey();
            nonzeroValues[index] = entry.getValue();
            index++;
        }
        return Vectors.sparse(dimension, indices, nonzeroValues);
    }

    private static Map<Integer, Double> coefficients(double[] values) {
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        for (int index = 0; index < values.length; index++) {
            if (Double.compare(values[index], 0.0d) != 0) {
                coefficients.put(index, values[index]);
            }
        }
        return coefficients;
    }

    private static Map<String, Double> trainingMetrics(ColumnTrainingDataset dataset) {
        Map<String, Double> metrics = new LinkedHashMap<String, Double>();
        metrics.put("sampleCount", (double) dataset.getExamples().size());
        metrics.put("positiveCount", (double) dataset.getPositiveCount());
        metrics.put("negativeCount", (double) dataset.getNegativeCount());
        metrics.put("conflictingCellCount", (double) dataset.getConflictingCellCount());
        return metrics;
    }
}
