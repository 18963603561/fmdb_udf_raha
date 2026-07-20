package com.fiberhome.ml.raha.job.stage.data;

import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.service.train.TrainingInputMergeService;
import com.fiberhome.ml.raha.service.train.TrainingBatchReference;
import com.fiberhome.ml.raha.service.train.TrainingMergeRequest;
import com.fiberhome.ml.raha.service.train.TrainingMergeResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 在画像之前把持久化 c1 和标注批次合并进当前 o1，并替换后续训练输入。
 */
public final class TrainingInputMergeStageHandler implements StageHandler {

    /** c1、标注批次和 o1 合并服务。 */
    private final TrainingInputMergeService mergeService;
    /** 一个或多个已经解析完成的采样和标注批次引用。 */
    private final List<TrainingBatchReference> batchReferences;
    /** c1 和 o1 共用的行身份配置。 */
    private final RowIdentityConfig rowIdentityConfig;

    public TrainingInputMergeStageHandler(
            TrainingInputMergeService mergeService,
            String sampleBatchId,
            String samplePartitionMonth,
            String annotationBatchId,
            String annotationPartitionMonth,
            RowIdentityConfig rowIdentityConfig) {
        this(mergeService, Collections.singletonList(
                new TrainingBatchReference(sampleBatchId, samplePartitionMonth,
                        annotationBatchId, annotationPartitionMonth)),
                rowIdentityConfig);
    }

    public TrainingInputMergeStageHandler(
            TrainingInputMergeService mergeService,
            List<TrainingBatchReference> batchReferences,
            RowIdentityConfig rowIdentityConfig) {
        if (mergeService == null || batchReferences == null
                || batchReferences.isEmpty() || rowIdentityConfig == null) {
            throw new IllegalArgumentException("训练合并阶段依赖和批次引用不能为空");
        }
        this.mergeService = mergeService;
        this.batchReferences = Collections.unmodifiableList(
                new ArrayList<TrainingBatchReference>(batchReferences));
        this.rowIdentityConfig = rowIdentityConfig;
    }

    @Override
    public StageType getStageType() {
        return StageType.MERGE_TRAINING_INPUT;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object value = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        if (!(value instanceof RahaDataset)) {
            return StageResult.failure("TRAINING_SOURCE_REQUIRED",
                    "训练合并阶段缺少当前 o1 数据集", false, 0L, 0L);
        }
        TrainingMergeResult result = mergeService.merge(new TrainingMergeRequest(
                context.getJob().getJobId(), (RahaDataset) value, rowIdentityConfig,
                batchReferences, context.getConfig().getResourceConfig()
                        .getBroadcastThresholdBytes()));
        context.getAttributes().put(StageAttributeKeys.RAHA_DATASET,
                result.getDataset());
        context.getAttributes().put(StageAttributeKeys.CELL_LABELS,
                result.getDirectLabels());
        context.getAttributes().put(StageAttributeKeys.TRAINING_MERGE_RESULT,
                result);
        // 任务快照仍绑定外部 o1，训练快照通过合并结果单独传递，避免中途改写输入身份。
        return StageResult.success();
    }
}
