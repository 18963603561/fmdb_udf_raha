package com.fiberhome.ml.raha.model;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * 在 Spark Executor 分区内计算列级模型分数，避免将大列特征收集到 Driver。
 */
public final class PartitionedColumnModelPredictor {

    /**
     * 对包含单元格标识和 ML 向量的数据集执行惰性分区预测。
     *
     * @param featureFrame 特征数据集，必须包含指定标识列和向量列
     * @param cellIdColumn 单元格标识列名称
     * @param featuresColumn ML 向量列名称
     * @param model 可移植列级模型
     * @param targetPartitionCount 目标分区数量
     * @return 附加分数、阈值、判断和模型元数据的惰性数据集
     */
    public Dataset<Row> predict(Dataset<Row> featureFrame,
                                String cellIdColumn,
                                String featuresColumn,
                                ColumnModelArtifact model,
                                int targetPartitionCount) {
        if (featureFrame == null || model == null || targetPartitionCount <= 0
                || cellIdColumn == null || cellIdColumn.trim().isEmpty()
                || featuresColumn == null || featuresColumn.trim().isEmpty()) {
            throw new IllegalArgumentException("分区预测输入、列名、模型和分区数必须有效");
        }
        if (!hasColumn(featureFrame, cellIdColumn)
                || !hasColumn(featureFrame, featuresColumn)) {
            throw new IllegalArgumentException("分区预测数据集缺少标识列或特征列");
        }
        int[] indices = new int[model.getCoefficients().size()];
        double[] coefficients = new double[model.getCoefficients().size()];
        int position = 0;
        for (Map.Entry<Integer, Double> entry : model.getCoefficients().entrySet()) {
            indices[position] = entry.getKey();
            coefficients[position] = entry.getValue();
            position++;
        }
        UDF1<Vector, Double> scorer = new LinearScoreFunction(
                indices, coefficients, model.getIntercept(), model.getFeatureDimension());
        Dataset<Row> partitioned = featureFrame.repartition(targetPartitionCount);
        Dataset<Row> scored = partitioned.withColumn("score", functions.udf(
                scorer, DataTypes.DoubleType).apply(functions.col(featuresColumn)));
        return scored
                .withColumn("threshold", functions.lit(model.getThreshold()))
                .withColumn("error", functions.col("score").geq(model.getThreshold()))
                .withColumn("classifier_type", functions.lit(
                        model.getClassifierType().name()))
                .withColumn("model_version", functions.lit(model.getModelVersion()));
    }

    private static boolean hasColumn(Dataset<Row> dataset, String columnName) {
        return Arrays.asList(dataset.columns()).contains(columnName);
    }

    private static final class LinearScoreFunction
            implements UDF1<Vector, Double>, Serializable {
        private static final long serialVersionUID = 1L;
        /** 非零系数对应的特征编号。 */
        private final int[] indices;
        /** 非零线性系数。 */
        private final double[] coefficients;
        /** 逻辑函数截距。 */
        private final double intercept;
        /** 模型特征维度。 */
        private final int dimension;

        private LinearScoreFunction(int[] indices,
                                    double[] coefficients,
                                    double intercept,
                                    int dimension) {
            this.indices = indices;
            this.coefficients = coefficients;
            this.intercept = intercept;
            this.dimension = dimension;
        }

        @Override
        public Double call(Vector vector) {
            if (vector == null || vector.size() != dimension) {
                throw new IllegalArgumentException("分区预测向量维度与模型不一致");
            }
            double linear = intercept;
            for (int index = 0; index < indices.length; index++) {
                linear += coefficients[index] * vector.apply(indices[index]);
            }
            if (linear >= 35.0d) {
                return 1.0d;
            }
            if (linear <= -35.0d) {
                return 0.0d;
            }
            return 1.0d / (1.0d + Math.exp(-linear));
        }
    }
}
