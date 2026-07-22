package com.fiberhome.ml.raha.service.task.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fiberhome.ml.raha.config.dto.RahaJobConfig;
import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionRequest;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * 验证列批配置和训练子请求的版本、字段范围与 RVD 边界。
 */
class ColumnBatchRequestTest {

    @Test
    void shouldCreateTrainingChildWithSharedVersionsAndRestrictedColumns() {
        RowIdentityConfig identity = RowIdentityConfig.contentHash();
        RahaJobConfig parentConfig = RahaJobConfig.defaults(JobType.TRAINING,
                "dataset", "dw.wide_table", identity)
                .withExecutionInputFingerprint("parent-fingerprint");
        DataLoadRequest loadRequest = new DataLoadRequest("dataset",
                "dw.wide_table", "dw.wide_table", identity,
                DataFormat.FMDB_TABLE, null, null, null, null, null, null);
        RahaTaskExecutionRequest parent = RahaTaskExecutionRequest.training(
                parentConfig, loadRequest, Collections.emptyList())
                .withColumnBatchOptions(new ColumnBatchOptions(
                        10, 1, false, false));
        ColumnBatch batch = new ColumnBatch(0, "parent-column-batch-001",
                Arrays.asList("c1", "c2"));
        Set<String> columns = new LinkedHashSet<String>(batch.getColumns());
        RahaJobConfig childConfig = parentConfig.withStrategyConfig(
                parentConfig.getStrategyConfig().withColumnBatch(columns, false))
                .withExecutionInputFingerprint("child-fingerprint");

        RahaTaskExecutionRequest child = parent.toColumnBatchChild(
                childConfig, loadRequest.withIncludedColumns(columns),
                new ColumnBatchContext("parent", batch, 1),
                "model-set-parent", "plan-parent", null);

        assertTrue(child.isColumnBatchChild());
        assertFalse(child.getColumnBatchOptions().isEnabled());
        assertEquals(columns, child.getDataLoadRequest().getIncludedColumns());
        assertEquals("model-set-parent", child.getModelSetVersionOverride());
        assertEquals("plan-parent",
                child.getModelCompatibilityVersionOverride());
    }

    @Test
    void shouldKeepRvdInsideBatchAndRejectParallelBatches() {
        StrategyConfig defaults = StrategyConfig.defaults();
        Set<String> columns = new LinkedHashSet<String>(
                Arrays.asList("c1", "c2", "c3"));

        StrategyConfig enabled = defaults.withColumnBatch(columns, true);
        StrategyConfig disabled = enabled.withColumnBatch(columns, false);

        assertEquals(columns, enabled.getIncludedColumns());
        assertTrue(enabled.getStrategyFamilies().contains(StrategyFamily.RVD));
        assertFalse(disabled.getStrategyFamilies().contains(StrategyFamily.RVD));
        assertThrows(IllegalArgumentException.class,
                () -> new ColumnBatchOptions(10, 2, false, false));
    }
}
