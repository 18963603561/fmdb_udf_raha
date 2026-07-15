package com.fiberhome.ml.raha.data.loader;

/**
 * 将外部数据源读取为 Raha 只读数据集的抽象接口。
 */
public interface RahaDatasetLoader {

    LoadedDataset load(DataLoadRequest request);
}

