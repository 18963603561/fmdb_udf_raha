package com.fiberhome.ml.raha.service.task.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * 验证列批规划的顺序、边界和参数校验。
 */
class ColumnBatchPlannerTest {

    @Test
    void shouldPreserveSchemaOrderAndSplitByConfiguredSize() {
        List<String> columns = new ArrayList<String>();
        for (int index = 1; index <= 23; index++) {
            columns.add("c" + index);
        }
        columns.add("c3");

        List<ColumnBatch> batches = new ColumnBatchPlanner().plan(
                "parent-1", columns, 10);

        assertEquals(3, batches.size());
        assertEquals(10, batches.get(0).getColumns().size());
        assertEquals("c1", batches.get(0).getColumns().get(0));
        assertEquals("c10", batches.get(0).getColumns().get(9));
        assertEquals("c11", batches.get(1).getColumns().get(0));
        assertEquals(3, batches.get(2).getColumns().size());
        assertEquals("parent-1-column-batch-003",
                batches.get(2).getBatchId());
    }

    @Test
    void shouldRejectEmptyColumnsAndInvalidBatchSize() {
        ColumnBatchPlanner planner = new ColumnBatchPlanner();
        assertThrows(IllegalArgumentException.class,
                () -> planner.plan("parent", java.util.Collections.emptyList(), 10));
        assertThrows(IllegalArgumentException.class,
                () -> planner.plan("parent",
                        java.util.Collections.singletonList("c1"), 0));
    }
}
