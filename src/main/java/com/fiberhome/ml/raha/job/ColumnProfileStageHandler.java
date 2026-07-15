package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.StageType;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.repository.ArtifactVersion;

/**
 * 读取加载阶段的数据集，生成列画像并写回新的数据集版本。
 */
public final class ColumnProfileStageHandler implements StageHandler {

    /** 列画像服务。 */
    private final ColumnProfileService profileService;

    public ColumnProfileStageHandler(ColumnProfileService profileService) {
        if (profileService == null) {
            throw new IllegalArgumentException("列画像服务不能为空");
        }
        this.profileService = profileService;
    }

    @Override
    public StageType getStageType() {
        return StageType.PROFILE;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object value = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        if (!(value instanceof RahaDataset)) {
            return StageResult.failure("DATASET_REQUIRED",
                    "列画像阶段缺少已加载数据集", false, 0L, 0L);
        }
        RahaDataset dataset = (RahaDataset) value;
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), dataset.getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        RahaDataset profiledDataset = profileService.profileAndSave(dataset, version);
        context.getAttributes().put(StageAttributeKeys.RAHA_DATASET, profiledDataset);
        return StageResult.success();
    }
}

