package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.FeatureType;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.feature.FeatureDefinition;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 Spark MLlib 逻辑回归、规则降级和列级预测输出。
 */
class ColumnModelTrainerIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldTrainMllibLogisticRegressionAndPredictScores() {
        TrainingFixture fixture = fixture();
        ColumnModelTrainingRequest request = request(fixture.dataset,
                ClassifierType.LOGISTIC_REGRESSION, true, 0.6d);
        ColumnModelTrainer trainer = new SparkMllibLogisticRegressionTrainer(
                SparkTestSession.get(), new ColumnModelVersioner());

        ColumnModelTrainingResult result = trainer.train(request);

        assertEquals(ColumnModelTrainingStatus.TRAINED, result.getStatus());
        assertFalse(result.isFallback());
        assertNotNull(result.getArtifact());
        assertEquals(ClassifierType.LOGISTIC_REGRESSION,
                result.getArtifact().getClassifierType());
        List<ColumnPrediction> predictions = new ColumnModelPredictor().predict(
                result.getArtifact(), fixture.rows);
        assertEquals(fixture.rows.size(), predictions.size());
        assertEquals(0.6d, predictions.get(0).getThreshold(), 0.000001d);
        assertEquals(ClassifierType.LOGISTIC_REGRESSION,
                predictions.get(0).getClassifierType());
        assertTrue(predictions.get(5).getScore() > predictions.get(0).getScore());
        assertTrue(predictions.stream().anyMatch(ColumnPrediction::isError));
    }

    @Test
    void shouldFallbackToWeightedRuleWhenMllibTrainingFails() {
        TrainingFixture fixture = fixture();
        ColumnModelTrainingRequest request = request(fixture.dataset,
                ClassifierType.LOGISTIC_REGRESSION, true, 0.5d);
        ColumnModelTrainer adaptive = new AdaptiveColumnModelTrainer(
                ignored -> new ColumnModelTrainingResult(
                        ColumnModelTrainingStatus.FAILED, null, false,
                        "模拟 MLlib 训练失败", null),
                new WeightedRuleFallbackTrainer(new ColumnModelVersioner()));

        ColumnModelTrainingResult result = adaptive.train(request);

        assertEquals(ColumnModelTrainingStatus.TRAINED, result.getStatus());
        assertTrue(result.isFallback());
        assertEquals(ClassifierType.WEIGHTED_RULE,
                result.getArtifact().getClassifierType());
        assertEquals("fallback_weighted_feature_rule",
                result.getArtifact().getTrainingMode());
        List<ColumnPrediction> predictions = new ColumnModelPredictor().predict(
                result.getArtifact(), fixture.rows);
        assertTrue(predictions.get(5).getScore() > predictions.get(0).getScore());
    }

    @Test
    void shouldReturnMllibFailureWhenFallbackIsDisabled() {
        TrainingFixture fixture = fixture();
        ColumnModelTrainingRequest request = request(fixture.dataset,
                ClassifierType.LOGISTIC_REGRESSION, false, 0.5d);
        ColumnModelTrainer adaptive = new AdaptiveColumnModelTrainer(
                ignored -> new ColumnModelTrainingResult(
                        ColumnModelTrainingStatus.FAILED, null, false,
                        "模拟 MLlib 训练失败", null),
                new WeightedRuleFallbackTrainer(new ColumnModelVersioner()));

        ColumnModelTrainingResult result = adaptive.train(request);

        assertEquals(ColumnModelTrainingStatus.FAILED, result.getStatus());
        assertNull(result.getArtifact());
    }

    private static ColumnModelTrainingRequest request(ColumnTrainingDataset dataset,
                                                      ClassifierType classifierType,
                                                      boolean fallbackEnabled,
                                                      double threshold) {
        return new ColumnModelTrainingRequest("raha-code", "dataset", "schema-v1",
                "plan-v1", dataset,
                new ModelConfig(classifierType, threshold, fallbackEnabled),
                new LogisticRegressionTrainingConfig(true, 50, 0.0d, 0.0d));
    }

    private static TrainingFixture fixture() {
        FeatureDictionary dictionary = dictionary();
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        List<CellLabel> labels = new ArrayList<CellLabel>();
        double[] signals = new double[]{0.1d, 0.2d, 0.3d, 3.0d, 4.0d, 5.0d};
        for (int index = 0; index < signals.length; index++) {
            String cellId = "c" + (index + 1);
            Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
            values.put(0, signals[index]);
            values.put(1, index < 3 ? 1.0d : 0.0d);
            rows.add(new SparseFeatureRow(cellId, "code", dictionary.getVersion(),
                    values, Collections.<String, String>emptyMap()));
            labels.add(new CellLabel(cellId, index < 3 ? 0 : 1,
                    LabelSource.HUMAN, 1.0d, null, null,
                    "tester", 1000L + index));
        }
        ColumnTrainingDataset dataset = new ColumnTrainingDataBuilder().build(
                "code", dictionary, rows, labels, true);
        return new TrainingFixture(rows, dataset);
    }

    private static FeatureDictionary dictionary() {
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(
                0, "anomaly-score", FeatureType.NUMERIC, "strategy", 0.0d));
        definitions.put(1, new FeatureDefinition(
                1, "normal-context", FeatureType.NUMERIC, "context", 0.0d));
        return new FeatureDictionary("dictionary-v1", "code", definitions, 1L);
    }

    private static final class TrainingFixture {
        /** 用于训练后预测的特征行。 */
        private final List<SparseFeatureRow> rows;
        /** 已完成列级关联和权重计算的训练集。 */
        private final ColumnTrainingDataset dataset;

        private TrainingFixture(List<SparseFeatureRow> rows,
                                ColumnTrainingDataset dataset) {
            this.rows = rows;
            this.dataset = dataset;
        }
    }
}
