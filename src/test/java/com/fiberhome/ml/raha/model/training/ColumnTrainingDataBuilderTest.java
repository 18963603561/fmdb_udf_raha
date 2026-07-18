package com.fiberhome.ml.raha.model.training;

import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.LabelPropagationMethod;
import com.fiberhome.ml.raha.model.release.ColumnModelVersioner;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证列级标签特征关联、直接标签优先、类别权重和不可训练列处理。
 */
class ColumnTrainingDataBuilderTest {

    @Test
    void shouldPreferDirectLabelsAndCombineClassAndSampleWeights() {
        FeatureDictionary dictionary = dictionary();
        List<SparseFeatureRow> rows = Arrays.asList(
                row("c1", values(1.0d, 0.0d), dictionary),
                row("c2", values(0.0d, 1.0d), dictionary),
                row("c3", values(0.0d, 2.0d), dictionary));
        List<CellLabel> labels = Arrays.asList(
                direct("c1", 1, 1000L),
                propagated("c1", 0, 0.2d, 1001L),
                direct("c2", 0, 1002L),
                propagated("c3", 0, 0.4d, 1003L));

        ColumnTrainingDataset dataset = new ColumnTrainingDataBuilder().build(
                "code", dictionary, rows, labels, true);

        assertEquals(ColumnTrainingStatus.TRAINABLE, dataset.getStatus());
        assertEquals(1, dataset.getPositiveCount());
        assertEquals(2, dataset.getNegativeCount());
        ColumnTrainingExample first = example(dataset, "c1");
        ColumnTrainingExample second = example(dataset, "c2");
        ColumnTrainingExample third = example(dataset, "c3");
        assertEquals(LabelSource.HUMAN, first.getLabelSource());
        assertEquals(1, first.getLabel());
        assertEquals(1.5d, first.getSampleWeight(), 0.000001d);
        assertEquals(0.75d, second.getSampleWeight(), 0.000001d);
        assertEquals(LabelSource.PROPAGATED, third.getLabelSource());
        assertEquals(0.3d, third.getSampleWeight(), 0.000001d);
        assertTrue(third.getSampleWeight() < second.getSampleWeight());
    }

    @Test
    void shouldExcludeConflictingDirectLabels() {
        FeatureDictionary dictionary = dictionary();
        List<CellLabel> labels = Arrays.asList(
                direct("c1", 1, 1000L), direct("c1", 0, 1001L));

        ColumnTrainingDataset dataset = new ColumnTrainingDataBuilder().build(
                "code", dictionary,
                Collections.singletonList(row("c1", values(1.0d, 0.0d), dictionary)),
                labels, true);

        assertEquals(ColumnTrainingStatus.LABEL_CONFLICT, dataset.getStatus());
        assertEquals(1, dataset.getConflictingCellCount());
        assertTrue(dataset.getExamples().isEmpty());
    }

    @Test
    void shouldNotCreateModelForSingleClassColumn() {
        FeatureDictionary dictionary = dictionary();
        ColumnTrainingDataset dataset = new ColumnTrainingDataBuilder().build(
                "code", dictionary,
                Collections.singletonList(row("c1", values(1.0d, 0.0d), dictionary)),
                Collections.singletonList(direct("c1", 1, 1000L)), true);
        ColumnModelTrainingRequest request = request(dataset, true);

        ColumnModelTrainingResult result = new WeightedRuleFallbackTrainer(
                new ColumnModelVersioner()).train(request);

        assertEquals(ColumnTrainingStatus.SINGLE_CLASS, dataset.getStatus());
        assertEquals(ColumnModelTrainingStatus.SINGLE_CLASS, result.getStatus());
        assertNull(result.getArtifact());
    }

    @Test
    void shouldNotCreateModelForEmptyFeatureColumn() {
        FeatureDictionary dictionary = new FeatureDictionary(
                "empty-v1", "code",
                Collections.<Integer, FeatureDefinition>emptyMap(), 1L);
        ColumnTrainingDataset dataset = new ColumnTrainingDataBuilder().build(
                "code", dictionary,
                Collections.singletonList(row("c1", Collections.<Integer, Double>emptyMap(),
                        dictionary)),
                Collections.singletonList(direct("c1", 1, 1000L)), true);

        ColumnModelTrainingResult result = new WeightedRuleFallbackTrainer(
                new ColumnModelVersioner()).train(request(dataset, true));

        assertEquals(ColumnTrainingStatus.EMPTY_FEATURES, dataset.getStatus());
        assertEquals(ColumnModelTrainingStatus.EMPTY_FEATURES, result.getStatus());
        assertNull(result.getArtifact());
    }

    private static ColumnModelTrainingRequest request(ColumnTrainingDataset dataset,
                                                      boolean fallbackEnabled) {
        return new ColumnModelTrainingRequest("raha-code", "dataset", "schema-v1",
                "plan-v1", dataset,
                new ModelConfig(ClassifierType.LOGISTIC_REGRESSION, 0.5d,
                        fallbackEnabled), LogisticRegressionTrainingConfig.defaults());
    }

    private static FeatureDictionary dictionary() {
        Map<Integer, FeatureDefinition> definitions =
                new LinkedHashMap<Integer, FeatureDefinition>();
        definitions.put(0, new FeatureDefinition(
                0, "strategy-hit", FeatureType.NUMERIC, "strategy", 0.0d));
        definitions.put(1, new FeatureDefinition(
                1, "context-score", FeatureType.NUMERIC, "context", 0.0d));
        return new FeatureDictionary("dictionary-v1", "code", definitions, 1L);
    }

    private static SparseFeatureRow row(String cellId,
                                        Map<Integer, Double> values,
                                        FeatureDictionary dictionary) {
        return new SparseFeatureRow(cellId, "code", dictionary.getVersion(),
                values, Collections.<String, String>emptyMap());
    }

    private static CellLabel direct(String cellId, int label, long createdAt) {
        return new CellLabel(cellId, label, LabelSource.HUMAN,
                1.0d, null, null, "tester", createdAt);
    }

    private static CellLabel propagated(String cellId,
                                        int label,
                                        double weight,
                                        long createdAt) {
        return new CellLabel("propagated-" + cellId + "-" + createdAt,
                cellId, label, LabelSource.PROPAGATED, 0.8d,
                "source-label", "cluster-a", "cluster-v1",
                LabelPropagationMethod.MAJORITY, weight, 1, 0.8d,
                "label-propagation", createdAt);
    }

    private static ColumnTrainingExample example(ColumnTrainingDataset dataset,
                                                  String cellId) {
        for (ColumnTrainingExample example : dataset.getExamples()) {
            if (example.getCellId().equals(cellId)) {
                return example;
            }
        }
        return null;
    }

    private static Map<Integer, Double> values(double first, double second) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        if (first != 0.0d) {
            values.put(0, first);
        }
        if (second != 0.0d) {
            values.put(1, second);
        }
        return values;
    }
}
