package com.fiberhome.ml.raha.model;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.linalg.VectorUDT;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 提供树模型训练器共用的 Spark 数据集转换和指标构造逻辑。
 */
final class SparkMllibTreeTrainerSupport {

    private SparkMllibTreeTrainerSupport() {
    }

    static Dataset<Row> trainingFrame(SparkSession sparkSession,
                                      ColumnTrainingDataset dataset) {
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

    static Map<String, Double> metrics(ColumnTrainingDataset dataset) {
        Map<String, Double> metrics = new LinkedHashMap<String, Double>();
        metrics.put("sampleCount", (double) dataset.getExamples().size());
        metrics.put("positiveCount", (double) dataset.getPositiveCount());
        metrics.put("negativeCount", (double) dataset.getNegativeCount());
        metrics.put("conflictingCellCount", (double) dataset.getConflictingCellCount());
        return metrics;
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
        return org.apache.spark.ml.linalg.Vectors.sparse(
                dimension, indices, nonzeroValues);
    }
}
