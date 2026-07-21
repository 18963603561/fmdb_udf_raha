package com.fiberhome.ml.raha.service.sample;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.util.HashUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ActiveSamplingOrchestratorTest {

    @Test
    void shouldLimitBudgetToRemainingCandidateRows() {
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        rows.add(row("1", "first"));
        rows.add(row("1", "second"));
        rows.add(row("2", "first"));
        rows.add(row("3", "first"));
        FeatureAssemblyResult features = new FeatureAssemblyResult(
                Collections.emptyMap(), rows,
                new FeatureAssemblyMetrics(rows.size(), 0L, 0L, 0L));
        List<CellLabel> labels = Collections.singletonList(new CellLabel(
                rows.get(0).getCellId(), 0, LabelSource.HUMAN,
                1.0d, null, null, "tester", 1L));

        assertEquals(3, ActiveSamplingOrchestrator.effectiveBudget(
                features, Collections.<CellLabel>emptyList(), 20));
        assertEquals(2, ActiveSamplingOrchestrator.effectiveBudget(
                features, labels, 20));
        assertEquals(1, ActiveSamplingOrchestrator.effectiveBudget(
                features, labels, 1));
    }

    @Test
    void shouldRejectInvalidBudgetCalculationInput() {
        assertThrows(IllegalArgumentException.class,
                () -> ActiveSamplingOrchestrator.effectiveBudget(
                        null, Collections.<CellLabel>emptyList(), 1));
    }

    private static SparseFeatureRow row(String rowId, String columnName) {
        CellCoordinate coordinate = new CellCoordinate(
                "dataset", "snapshot", rowId, columnName);
        return new SparseFeatureRow(coordinate.toCellId(), columnName,
                coordinate, HashUtils.md5Hex(rowId + "|" + columnName),
                null, "dictionary-version", Collections.emptyMap(),
                Collections.emptyMap());
    }
}
