package com.fiberhome.ml.raha.performance;

import java.util.Arrays;
import java.util.Collections;
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
        return Collections.unmodifiableList(Arrays.asList(
                new BenchmarkDatasetSpec("small-low-error", BenchmarkScale.SMALL,
                        100000L, 10, 0.001d, 8, 101L),
                new BenchmarkDatasetSpec("medium-normal-error", BenchmarkScale.MEDIUM,
                        1000000L, 20, 0.01d, 64, 202L),
                new BenchmarkDatasetSpec("large-high-error", BenchmarkScale.LARGE,
                        10000000L, 30, 0.05d, 400, 303L),
                new BenchmarkDatasetSpec("wide-normal-error", BenchmarkScale.WIDE,
                        1000000L, 200, 0.01d, 160, 404L)));
    }
}
