package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.DatasetSnapshot;
import com.fiberhome.ml.raha.data.RahaDataset;

/**
 * 返回只读 Raha 数据集及其输入快照元数据。
 */
public final class LoadedDataset {

    /** 已加载的只读 Raha 数据集。 */
    private final RahaDataset dataset;
    /** 本次加载对应的快照元数据。 */
    private final DatasetSnapshot snapshot;

    public LoadedDataset(RahaDataset dataset, DatasetSnapshot snapshot) {
        if (dataset == null || snapshot == null) {
            throw new IllegalArgumentException("已加载数据集和快照不能为空");
        }
        this.dataset = dataset;
        this.snapshot = snapshot;
    }

    public RahaDataset getDataset() {
        return dataset;
    }

    public DatasetSnapshot getSnapshot() {
        return snapshot;
    }
}

