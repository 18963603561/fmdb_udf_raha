package com.fiberhome.ml.raha.parallel;

import com.fiberhome.ml.raha.config.ResourceConfig;
import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.model.ColumnModelArtifact;
import com.fiberhome.ml.raha.model.PartitionedColumnModelPredictor;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 Spark 分区预测以及广播、缓存大小上限控制。
 */
class SparkParallelResourceIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldPredictInsideTargetPartitionsWithoutDriverCollection() {
        SparkSession spark = SparkTestSession.get();
        Dataset<Row> featureFrame = featureFrame(spark);
        Dataset<Row> predictions = new PartitionedColumnModelPredictor().predict(
                featureFrame, "cell_id", "features", model(), 3);

        assertEquals(3, predictions.javaRDD().getNumPartitions());
        assertEquals(20L, predictions.count());
        assertEquals(10L, predictions.filter("error = true").count());
        assertTrue(Arrays.asList(predictions.columns()).contains("score"));
        assertTrue(Arrays.asList(predictions.columns()).contains("model_version"));
        Row first = predictions.orderBy("cell_id").first();
        assertEquals("cell-00", first.getAs("cell_id"));
        assertEquals(0.5d, (Double) first.getAs("score"), 0.0000001d);
    }

    @Test
    void shouldRefuseOversizedBroadcastAndCache() {
        SparkSession spark = SparkTestSession.get();
        ResourceConfig config = new ResourceConfig(
                2, 2, 10L, "MEMORY_ONLY", 20L, 5000L);
        SparkResourceManager manager = new SparkResourceManager();

        Optional<Broadcast<String>> accepted = manager.broadcastIfAllowed(
                spark, "small", 10L, config);
        Optional<Broadcast<String>> refused = manager.broadcastIfAllowed(
                spark, "large", 11L, config);

        assertTrue(accepted.isPresent());
        assertEquals("small", accepted.get().value());
        assertFalse(refused.isPresent());
        accepted.get().destroy();

        Dataset<Row> dataset = spark.range(0L, 5L).toDF();
        assertEquals(StorageLevel.NONE(), dataset.storageLevel());
        assertFalse(manager.persistIfAllowed(dataset, 21L, config));
        assertEquals(StorageLevel.NONE(), dataset.storageLevel());
        assertTrue(manager.persistIfAllowed(dataset, 20L, config));
        assertEquals(StorageLevel.MEMORY_ONLY(), dataset.storageLevel());
        dataset.count();
        dataset.unpersist(true);
    }

    private static Dataset<Row> featureFrame(SparkSession spark) {
        List<Row> rows = new ArrayList<Row>();
        for (int index = 0; index < 20; index++) {
            rows.add(RowFactory.create(String.format("cell-%02d", index),
                    index % 2 == 0 ? 0.0d : 1.0d, 0.0d));
        }
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("cell_id", DataTypes.StringType, false),
                DataTypes.createStructField("feature_0", DataTypes.DoubleType, false),
                DataTypes.createStructField("feature_1", DataTypes.DoubleType, false)
        });
        Dataset<Row> raw = spark.createDataFrame(rows, schema);
        return new VectorAssembler()
                .setInputCols(new String[]{"feature_0", "feature_1"})
                .setOutputCol("features")
                .transform(raw)
                .select("cell_id", "features");
    }

    private static ColumnModelArtifact model() {
        Map<Integer, Double> coefficients = new LinkedHashMap<Integer, Double>();
        coefficients.put(0, 2.0d);
        return new ColumnModelArtifact("raha-code", "model-v1", "code",
                ClassifierType.LOGISTIC_REGRESSION, "dictionary-v1", 2,
                0.6d, 0.0d, coefficients, "test-logistic");
    }
}
