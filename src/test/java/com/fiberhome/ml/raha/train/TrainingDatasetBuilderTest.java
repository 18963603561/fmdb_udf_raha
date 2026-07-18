package com.fiberhome.ml.raha.train;

import com.fiberhome.ml.raha.data.CellLabel;
import com.fiberhome.ml.raha.sample.SampleTuple;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.RahaException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 直接标签、传播标签和值漂移测试。
 */
class TrainingDatasetBuilderTest {

    @Test
    void shouldBuildDirectAndSameValuePropagationExamples() {
        SampleTuple first = tuple("1", "Mordor");
        SampleTuple second = tuple("2", "Mordor");
        CellLabel label = new CellLabel("sample", "toy", "snap", "1",
                "Kingdom", HashUtils.sha256("Mordor"), 1, 1L, "2026-07-17");
        List<RawTrainingExample> examples = new TrainingDatasetBuilder().build(
                Arrays.asList(first, second), Collections.singletonList(label),
                Collections.singletonList("Kingdom"));
        assertEquals(2, examples.size());
        assertEquals("DIRECT", examples.get(0).getLabelSource());
        assertEquals("PROPAGATED", examples.get(1).getLabelSource());
    }

    @Test
    void shouldRejectStaleValueHash() {
        CellLabel label = new CellLabel("sample", "toy", "snap", "1",
                "Kingdom", HashUtils.sha256("Rohan"), 1, 1L, "2026-07-17");
        assertThrows(RahaException.class, () -> new TrainingDatasetBuilder().build(
                Collections.singletonList(tuple("1", "Mordor")),
                Collections.singletonList(label),
                Collections.singletonList("Kingdom")));
    }

    private static SampleTuple tuple(String rowId, String value) {
        return new SampleTuple("sample", "toy", "snap", rowId, 1L,
                "{\"ID\":\"" + rowId + "\",\"Kingdom\":\"" + value + "\"}",
                1, 1.0d, "{}", 1L, "2026-07-17");
    }
}
