package com.fiberhome.ml.raha.job.stage.batch;

import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionRequest;
import com.fiberhome.ml.raha.service.task.RahaTaskExecutionResult;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatch;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchExecutionCoordinator;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchExecutionOutcome;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchExecutionSummary;
import java.util.List;
import java.util.function.Function;

/**
 * 在 driver 侧串行发起列批子任务并将结果写回父任务上下文。
 */
public final class ColumnBatchStageHandler implements StageHandler {

    /** 父任务原始请求。 */
    private final RahaTaskExecutionRequest parentRequest;
    /** 已规划的稳定列批。 */
    private final List<ColumnBatch> batches;
    /** 列批协调器。 */
    private final ColumnBatchExecutionCoordinator coordinator;
    /** 禁止再次拆批的单任务执行入口。 */
    private final Function<RahaTaskExecutionRequest, RahaTaskExecutionResult>
            childExecutor;

    public ColumnBatchStageHandler(
            RahaTaskExecutionRequest parentRequest,
            List<ColumnBatch> batches,
            ColumnBatchExecutionCoordinator coordinator,
            Function<RahaTaskExecutionRequest, RahaTaskExecutionResult>
                    childExecutor) {
        if (parentRequest == null || batches == null || batches.isEmpty()
                || coordinator == null || childExecutor == null) {
            throw new IllegalArgumentException("列批父任务阶段依赖不能为空");
        }
        this.parentRequest = parentRequest;
        this.batches = batches;
        this.coordinator = coordinator;
        this.childExecutor = childExecutor;
    }

    @Override
    public StageType getStageType() {
        return StageType.COLUMN_BATCH;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        ColumnBatchExecutionOutcome outcome = coordinator.execute(
                parentRequest, context.getJob().getJobId(), batches,
                childExecutor);
        context.getAttributes().putAll(outcome.getAttributes());
        ColumnBatchExecutionSummary summary = outcome.getSummary();
        if (summary.getSucceededBatchCount() == 0) {
            return StageResult.failure("ALL_COLUMN_BATCHES_FAILED",
                    "全部列批子任务执行失败", false,
                    summary.getBatchCount(), summary.getBatchCount());
        }
        if (!summary.getFailedColumns().isEmpty()) {
            return StageResult.partialSuccess("PARTIAL_COLUMN_BATCH_FAILURE",
                    "部分列批字段执行失败",
                    summary.getFailedColumns().size(),
                    summary.getFailedColumns().size()
                            + outcome.getSummary().getSucceededBatchCount());
        }
        return StageResult.success();
    }
}
