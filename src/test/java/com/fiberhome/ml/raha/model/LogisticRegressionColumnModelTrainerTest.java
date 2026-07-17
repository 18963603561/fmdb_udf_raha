package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.FeatureVectorizer;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.train.TrainingExample;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Spark 逻辑回归训练和预测接口测试。
 */
class LogisticRegressionColumnModelTrainerTest {

    /** 测试 Spark 会话。 */
    private static SparkSession spark;

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder().master("local[1]")
                .appName("raha-logistic-test")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "127.0.0.1")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void stopSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void shouldTrainAndPredictThroughExtensionInterfaces() {
        List<String> values = Arrays.asList("Mordor", "Rohan", "", "123", "Mordor3");
        FeatureDictionary dictionary = FeatureDictionary.build("Kingdom", values, 100);
        FeatureVectorizer vectorizer = new FeatureVectorizer();
        List<TrainingExample> examples = new ArrayList<TrainingExample>();
        examples.add(example("1", "Mordor", 0, dictionary, vectorizer));
        examples.add(example("2", "Rohan", 0, dictionary, vectorizer));
        examples.add(example("3", "", 1, dictionary, vectorizer));
        examples.add(example("4", "123", 1, dictionary, vectorizer));
        examples.add(example("5", "Mordor3", 1, dictionary, vectorizer));
        ColumnModelTrainer trainer = new LogisticRegressionColumnModelTrainer(spark,
                new ModelConfig(100, 0.01d, 0.5d));
        RahaColumnModel model = trainer.train("modelset", "toy", "Kingdom", null,
                dictionary, examples, 1L);
        ColumnModelPredictor predictor = new LogisticRegressionColumnModelPredictor();
        assertEquals(ClassifierType.LOGISTIC_REGRESSION, model.getClassifierType());
        assertTrue(predictor.predict(model, "123")
                > predictor.predict(model, "Mordor"));
    }

    private static TrainingExample example(String rowId, String value, int label,
                                            FeatureDictionary dictionary,
                                            FeatureVectorizer vectorizer) {
        return new TrainingExample("modelset", "toy", "sample", "Kingdom",
                "snap", rowId, 1L, HashUtils.sha256(value),
                vectorizer.vectorize(value, dictionary), label, "DIRECT", 1.0d,
                1L, "2026-07-17");
    }
}
