package com.fiberhome.ml.raha.data;

import com.fiberhome.ml.raha.cluster.ClusterAssignment;
import com.fiberhome.ml.raha.feature.FeatureDefinition;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.strategy.StrategyHit;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.util.HashUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证核心领域对象的定位、不可变性和检测边界。
 */
class DomainObjectTest {

    @Test
    void shouldGenerateStableCellId() {
        CellCoordinate first = new CellCoordinate("dataset", "snapshot", "row-1", "name");
        CellCoordinate second = new CellCoordinate("dataset", "snapshot", "row-1", "name");
        CellCoordinate otherColumn = new CellCoordinate("dataset", "snapshot", "row-1", "city");

        assertEquals(first.toCellId(), second.toCellId());
        assertNotEquals(first.toCellId(), otherColumn.toCellId());
        assertEquals(64, first.toCellId().length());
    }

    @Test
    void shouldKeepDatasetCollectionsImmutable() {
        ColumnMetadata metadata = new ColumnMetadata("id", 0, "string", false, true, false);
        ColumnProfile profile = new ColumnProfile(
                "id", 1L, 0L, 1L, 1, 1, 1.0d, Collections.singletonMap("number", 1L));
        RahaDataset dataset = new RahaDataset(
                "dataset", "snapshot", "table", "id",
                Collections.singletonList(metadata), null, "schema-hash",
                Collections.singletonMap("id", profile));

        assertNull(dataset.getDataFrame());
        assertThrows(UnsupportedOperationException.class,
                () -> dataset.getColumns().add(metadata));
        assertThrows(UnsupportedOperationException.class,
                () -> dataset.getProfiles().put("other", profile));
    }

    @Test
    void shouldCreateStrategyFeatureLabelAndModelObjects() {
        CellCoordinate coordinate = new CellCoordinate("dataset", "snapshot", "row-1", "name");
        CellValue cellValue = CellValue.of(coordinate, "bad-value", "b***e");
        StrategyPlan plan = new StrategyPlan(
                "strategy-1", StrategyFamily.PVD, Collections.singletonList("name"),
                Collections.singletonMap("pattern", "letters"), 1, StrategyStatus.PLANNED);
        StrategyHit hit = new StrategyHit(
                "job-1", "stage-1", plan.getStrategyId(), plan.getStrategyFamily(), coordinate,
                cellValue.getValueHash(), "PVD_PATTERN", Collections.singletonMap("actual", "mixed"),
                0.9d, 10L, StrategyStatus.SUCCEEDED);
        FeatureDefinition definition = new FeatureDefinition(
                0, "strategy.pvd.pattern.hit", FeatureType.BINARY, "strategy-1", 0.0d);
        FeatureDictionary dictionary = new FeatureDictionary(
                "feature-v1", "name", Collections.singletonMap(0, definition), 1L);
        SparseFeatureRow row = new SparseFeatureRow(
                coordinate.toCellId(), "name", dictionary.getVersion(),
                Collections.singletonMap(0, 1.0d), Collections.singletonMap("pvdHitCount", "1"));
        ClusterAssignment assignment = new ClusterAssignment(
                coordinate.toCellId(), "name", "cluster-1", "hierarchical", "cluster-v1", 0.0d);
        CellLabel label = new CellLabel(
                coordinate.toCellId(), 1, LabelSource.PROPAGATED, 0.8d,
                "label-1", assignment.getClusterId(), "system", 1L);
        RahaColumnModel model = new RahaColumnModel(
                "name-model", "model-v1", "dataset", "name", "schema-hash",
                ClassifierType.WEIGHTED_RULE, dictionary.getVersion(), "strategy-plan-v1",
                0.5d, null, ModelStatus.DRAFT, Collections.singletonMap("f1", 0.8d), 1L);

        assertEquals("PVD_PATTERN", hit.getReasonCode());
        assertEquals(1.0d, row.getValues().get(0));
        assertEquals(LabelSource.PROPAGATED, label.getLabelSource());
        assertEquals(ClassifierType.WEIGHTED_RULE, model.getClassifierType());
        assertNotNull(cellValue.getValueHash());
    }

    @Test
    void shouldRejectInvalidFeatureDictionaryAndPropagationLabel() {
        FeatureDefinition definition = new FeatureDefinition(
                1, "feature-1", FeatureType.BINARY, "strategy-1", 0.0d);
        Map<Integer, FeatureDefinition> mismatchedDefinitions = new LinkedHashMap<Integer, FeatureDefinition>();
        mismatchedDefinitions.put(0, definition);

        assertThrows(IllegalArgumentException.class,
                () -> new FeatureDictionary("v1", "name", mismatchedDefinitions, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new CellLabel("cell-1", 1, LabelSource.PROPAGATED,
                        0.8d, null, null, "system", 1L));
    }

    @Test
    void shouldRejectRawOrOversizedValueFrequencySummary() {
        Map<String, Long> oversizedFrequencies = new LinkedHashMap<String, Long>();
        for (int index = 0; index < 21; index++) {
            oversizedFrequencies.put(HashUtils.sha256Hex("value-" + index), 1L);
        }

        assertThrows(IllegalArgumentException.class,
                () -> completeProfile(Collections.singletonMap("raw-value", 1L)));
        assertThrows(IllegalArgumentException.class,
                () -> completeProfile(oversizedFrequencies));
    }

    @Test
    void shouldExposeDetectionSemanticsOnly() {
        CellCoordinate coordinate = new CellCoordinate("dataset", "snapshot", "row-1", "name");
        DetectionResult result = new DetectionResult(
                "job-1", coordinate, "value-hash", "b***e", true, 0.9d, 0.5d,
                Arrays.asList("strategy-1", "strategy-2"),
                Collections.singletonMap("reason", "pattern"),
                "model", "v1", "feature-v1", 1L);

        assertTrue(result.isError());
        assertEquals(2, result.getStrategyIds().size());
        for (Field field : DetectionResult.class.getDeclaredFields()) {
            String fieldName = field.getName().toLowerCase(Locale.ROOT);
            assertFalse(fieldName.contains("correct"));
            assertFalse(fieldName.contains("repair"));
            assertFalse(fieldName.contains("clean"));
        }
    }

    private static ColumnProfile completeProfile(Map<String, Long> valueHashFrequencies) {
        return new ColumnProfile("code", 1L, 0L, 0L, 1L,
                1, 1, 1.0d, 0L, 0.0d,
                null, null, null, null, null, null,
                Collections.singletonMap("LETTER", 1L), valueHashFrequencies);
    }
}
