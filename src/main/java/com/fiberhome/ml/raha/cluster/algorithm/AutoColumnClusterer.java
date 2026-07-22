package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按单列样本量自动选择精确聚类或可扩展近似聚类。
 *
 * <p>样本数不超过 {@code maxSampleCount} 时使用 Smile 层次聚类保证小样本质量；
 * 样本数超过上限时切换到可扩展近似聚类，避免精确层次聚类的平方级开销。</p>
 */
public final class AutoColumnClusterer implements ColumnClusterer {

    /** 自动路由聚类器的逻辑算法名。 */
    public static final String ALGORITHM = "auto_smile_or_scalable_v1";
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoColumnClusterer.class);

    /** 小样本精确聚类器。 */
    private final SmileHierarchicalColumnClusterer exactClusterer;
    /** 大样本近似聚类器。 */
    private final ScalableColumnClusterer scalableClusterer;

    public AutoColumnClusterer(ClusterVersioner versioner, Clock clock) {
        if (versioner == null || clock == null) {
            throw new IllegalArgumentException("自动聚类器依赖不能为空");
        }
        this.exactClusterer = new SmileHierarchicalColumnClusterer(versioner, clock);
        this.scalableClusterer = new ScalableColumnClusterer(versioner, clock);
    }

    @Override
    public String getAlgorithm() {
        return ALGORITHM;
    }

    @Override
    public ColumnClusteringResult cluster(String columnName,
                                          FeatureDictionary dictionary,
                                          List<SparseFeatureRow> rows,
                                          ClusteringConfig config,
                                          long randomSeed) {
        if (rows == null || config == null) {
            throw new IllegalArgumentException("自动聚类输入和配置不能为空");
        }
        if (rows.size() <= config.getMaxSampleCount()) {
            LOGGER.info("AUTO 聚类选择 Smile 精确路径，columnName={}，rowCount={}，exactLimit={}",
                    columnName, rows.size(), config.getMaxSampleCount());
            return exactClusterer.cluster(columnName, dictionary, rows, config, randomSeed);
        }
        LOGGER.info("AUTO 聚类选择 Scalable 近似路径，columnName={}，rowCount={}，exactLimit={}，"
                        + "targetClusterCount={}",
                columnName, rows.size(), config.getMaxSampleCount(),
                config.getTargetClusterCount());
        return scalableClusterer.cluster(columnName, dictionary, rows, config, randomSeed);
    }
}
