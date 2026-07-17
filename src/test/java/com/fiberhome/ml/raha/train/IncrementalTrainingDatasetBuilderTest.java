package com.fiberhome.ml.raha.train;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 增量样本覆盖规则测试。
 */
class IncrementalTrainingDatasetBuilderTest {

    @Test
    void shouldLetCurrentExampleOverrideParentCell() {
        TrainingExample parent = example("parent", 0);
        TrainingExample current = example("current", 1);
        List<TrainingExample> merged = new IncrementalTrainingDatasetBuilder().merge(
                Collections.singletonList(parent), Collections.singletonList(current));
        assertEquals(1, merged.size());
        assertEquals(1, merged.get(0).getLabel());
        assertEquals("current", merged.get(0).getModelSetVersion());
    }

    private static TrainingExample example(String modelSetVersion, int label) {
        return new TrainingExample(modelSetVersion, "toy", "sample", "Kingdom",
                "snap", "1", 1L, "hash", new double[]{1.0d}, label,
                "DIRECT", 1.0d, 1L, "2026-07-17");
    }
}
