package com.fiberhome.ml.raha.data.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据数据加载请求的格式选择文件或 FMDB 数据加载器。
 */
public final class RoutingRahaDatasetLoader implements RahaDatasetLoader {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RoutingRahaDatasetLoader.class);
    /** 文件格式加载器。 */
    private final RahaDatasetLoader fileLoader;
    /** FMDB 表和 SQL 加载器。 */
    private final RahaDatasetLoader fmdbLoader;

    /**
     * 创建按请求格式路由的加载器。
     *
     * @param fileLoader 文件格式加载器
     * @param fmdbLoader FMDB 加载器
     */
    public RoutingRahaDatasetLoader(RahaDatasetLoader fileLoader,
                                    RahaDatasetLoader fmdbLoader) {
        if (fileLoader == null || fmdbLoader == null) {
            throw new IllegalArgumentException("数据加载路由器依赖不能为空");
        }
        this.fileLoader = fileLoader;
        this.fmdbLoader = fmdbLoader;
    }

    @Override
    public LoadedDataset load(DataLoadRequest request) {
        if (request == null || request.getFormat() == null) {
            throw new IllegalArgumentException("数据加载请求和格式不能为空");
        }
        if (request.getFormat().isFileFormat()) {
            LOGGER.debug("路由到文件数据加载器，datasetId={}，format={}",
                    request.getDatasetId(), request.getFormat());
            return fileLoader.load(request);
        }
        if (request.getFormat() == DataFormat.FMDB_TABLE
                || request.getFormat() == DataFormat.FMDB_SQL) {
            LOGGER.debug("路由到 FMDB 数据加载器，datasetId={}，format={}",
                    request.getDatasetId(), request.getFormat());
            return fmdbLoader.load(request);
        }
        throw new IllegalArgumentException("暂不支持的数据格式："
                + request.getFormat());
    }
}
