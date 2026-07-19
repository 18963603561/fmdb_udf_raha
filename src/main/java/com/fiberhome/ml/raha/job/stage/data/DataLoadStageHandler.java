package com.fiberhome.ml.raha.job.stage.data;

import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import com.fiberhome.ml.raha.data.loader.LoadedDataset;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.type.StageType;

/**
 * 执行外部数据读取并将只读数据集和快照写入阶段上下文。
 */
public final class DataLoadStageHandler implements StageHandler {

    /** 数据集加载器。 */
    private final RahaDatasetLoader datasetLoader;
    /** 当前任务的数据加载请求。 */
    private final DataLoadRequest loadRequest;

    public DataLoadStageHandler(RahaDatasetLoader datasetLoader, DataLoadRequest loadRequest) {
        if (datasetLoader == null || loadRequest == null) {
            throw new IllegalArgumentException("数据加载器和加载请求不能为空");
        }
        this.datasetLoader = datasetLoader;
        this.loadRequest = loadRequest;
    }

    @Override
    public StageType getStageType() {
        return StageType.LOAD_DATA;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        LoadedDataset loadedDataset = datasetLoader.load(loadRequest);
        context.getAttributes().put(StageAttributeKeys.RAHA_DATASET, loadedDataset.getDataset());
        context.getAttributes().put(StageAttributeKeys.DATASET_SNAPSHOT, loadedDataset.getSnapshot());
        return StageResult.successWithSnapshot(loadedDataset.getSnapshot().getSnapshotId());
    }
}
