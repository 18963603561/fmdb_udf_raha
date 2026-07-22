package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import java.util.List;

/**
 * 单列特征聚类统一接口，允许后续替换为 Spark 近似聚类实现。
 */
public interface ColumnClusterer {

    /**
     * 返回用于结果版本和持久化的稳定算法名称。
     *
     * @return 聚类算法名称
     */
    String getAlgorithm();

    /**
     * 判断当前聚类器是否允许通过本地线程池按列并发执行。
     *
     * <p>Spark MLlib 类聚类器会在 driver 侧提交 Spark 作业，不适合由本地线程池同时发起多列训练，
     * 因此需要通过该能力声明让上层服务降级为串行调度。</p>
     *
     * @return 是否支持本地列级并发
     */
    default boolean supportsLocalColumnParallelism() {
        return true;
    }

    /**
     * 对同一字段的单元格稀疏特征进行聚类。
     *
     * @param columnName 字段名称
     * @param dictionary 当前字段特征字典
     * @param rows 当前字段单元格特征
     * @param config 聚类配置
     * @param randomSeed 可复现随机种子
     * @return 单列聚类结果
     */
    ColumnClusteringResult cluster(String columnName,
                                   FeatureDictionary dictionary,
                                   List<SparseFeatureRow> rows,
                                   ClusteringConfig config,
                                   long randomSeed);
}
