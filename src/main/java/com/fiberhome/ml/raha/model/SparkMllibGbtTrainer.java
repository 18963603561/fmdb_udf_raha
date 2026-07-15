package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ClassifierType;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 Spark MLlib 训练二分类梯度提升树，并保存为可移植树结构。
 */
public final class SparkMllibGbtTrainer implements ColumnModelTrainer {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkMllibGbtTrainer.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** 模型版本生成器。 */
    private final ColumnModelVersioner versioner;

    public SparkMllibGbtTrainer(SparkSession sparkSession,
                                ColumnModelVersioner versioner) {
        if (sparkSession == null || versioner == null) {
            throw new IllegalArgumentException("梯度提升树训练器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.versioner = versioner;
    }

    @Override
    public ColumnModelTrainingResult train(ColumnModelTrainingRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("列级模型训练请求不能为空");
        }
        ColumnTrainingDataset dataset = request.getDataset();
        if (dataset.getStatus() != ColumnTrainingStatus.TRAINABLE) {
            return ColumnModelTrainingResult.untrainable(dataset);
        }
        LOGGER.info("开始 MLlib 梯度提升树训练，datasetId={}，columnName={}，sampleCount={}",
                request.getDatasetId(), dataset.getColumnName(), dataset.getExamples().size());
        try {
            TreeModelTrainingConfig config = request.getTreeTrainingConfig();
            Dataset<Row> frame = SparkMllibTreeTrainerSupport.trainingFrame(
                    sparkSession, dataset);
            GBTClassifier classifier = new GBTClassifier()
                    .setLabelCol("label")
                    .setFeaturesCol("features")
                    .setWeightCol("weight")
                    .setMaxDepth(config.getMaxDepth())
                    .setMaxBins(config.getMaxBins())
                    .setMinInstancesPerNode(config.getMinInstancesPerNode())
                    .setMinInfoGain(config.getMinInfoGain())
                    .setMaxIter(config.getMaxIterations())
                    .setStepSize(config.getStepSize())
                    .setSubsamplingRate(config.getSubsamplingRate());
            GBTClassificationModel model = classifier.fit(frame);
            String payload = TreeModelCodec.encode(model);
            String mode = "spark_mllib_gbt";
            String version = versioner.versionOfTree(request,
                    ClassifierType.GBT, mode, payload);
            ColumnModelArtifact artifact = new ColumnModelArtifact(
                    request.getModelName(), version, dataset.getColumnName(),
                    ClassifierType.GBT, dataset.getFeatureDictionaryVersion(),
                    dataset.getFeatureDimension(), request.getModelConfig().getThreshold(),
                    0.0d, java.util.Collections.<Integer, Double>emptyMap(), mode, payload);
            LOGGER.info("MLlib 梯度提升树训练完成，datasetId={}，columnName={}，modelVersion={}，treeCount={}",
                    request.getDatasetId(), dataset.getColumnName(), version, model.getNumTrees());
            return new ColumnModelTrainingResult(ColumnModelTrainingStatus.TRAINED,
                    artifact, false, "MLlib 梯度提升树训练完成",
                    SparkMllibTreeTrainerSupport.metrics(dataset));
        } catch (RuntimeException | LinkageError exception) {
            LOGGER.error("MLlib 梯度提升树训练失败，datasetId={}，columnName={}",
                    request.getDatasetId(), dataset.getColumnName(), exception);
            return new ColumnModelTrainingResult(ColumnModelTrainingStatus.FAILED,
                    null, false, "MLlib 梯度提升树训练失败："
                    + exception.getClass().getSimpleName(), null);
        }
    }
}
