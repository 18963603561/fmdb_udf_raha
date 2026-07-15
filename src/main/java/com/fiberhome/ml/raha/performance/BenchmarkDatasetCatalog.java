package com.fiberhome.ml.raha.performance;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;

import java.util.List;

/**
 * 提供覆盖规模、字段宽度和错误率维度的生产基准数据集定义。
 */
public final class BenchmarkDatasetCatalog {

    private BenchmarkDatasetCatalog() {
    }

    /**
     * 返回标准生产容量基准，调用方可按集群资源选择性执行。
     */
    public static List<BenchmarkDatasetSpec> standard() {
        return RahaDefaultConfigProvider.factory().benchmarkDatasetSpecs();
    }
}
