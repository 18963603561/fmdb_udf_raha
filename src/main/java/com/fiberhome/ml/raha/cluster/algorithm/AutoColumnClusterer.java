package com.fiberhome.ml.raha.cluster.algorithm;

import com.fiberhome.ml.raha.cluster.ClusterVersioner;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import java.time.Clock;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按单列样本量自动选择 Smile 精确聚类或 Spark KMeans 大样本聚类。
 *
 * <p>样本数不超过 {@code maxSampleCount} 时使用 Smile 层次聚类保证小样本质量；
 * 样本数超过上限且存在 Spark 会话时使用 Spark KMeans，由 driver 统一提交 Spark 作业。
 * 无 Spark 会话的测试或旧调用场景会回退到现有可扩展近似聚类。</p>
 */
public final class AutoColumnClusterer implements ColumnClusterer {

    /** 自动路由聚类器的逻辑算法名。 */
    public static final String ALGORITHM = "auto_smile_or_spark_kmeans_v1";
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoColumnClusterer.class);

    /** 小样本精确聚类器。 */
    private final SmileHierarchicalColumnClusterer exactClusterer;
    /** 无 Spark 会话时的大样本兜底近似聚类器。 */
    private final ScalableColumnClusterer scalableClusterer;
    /** Spark 大样本 KMeans 聚类器，无 Spark 会话时为空。 */
    private final SparkKMeansColumnClusterer sparkClusterer;

    public AutoColumnClusterer(ClusterVersioner versioner, Clock clock) {
        this(versioner, clock, null);
    }

    public AutoColumnClusterer(ClusterVersioner versioner,
                               Clock clock,
                               SparkSession sparkSession) {
        if (versioner == null || clock == null) {
            throw new IllegalArgumentException("自动聚类器依赖不能为空");
        }
        this.exactClusterer = new SmileHierarchicalColumnClusterer(versioner, clock);
        this.scalableClusterer = new ScalableColumnClusterer(versioner, clock);
        this.sparkClusterer = sparkSession == null
                ? null : new SparkKMeansColumnClusterer(sparkSession, versioner, clock);
    }

    @Override
    public String getAlgorithm() {
        return ALGORITHM;
    }

    @Override
    public boolean supportsLocalColumnParallelism() {
        return sparkClusterer == null;
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
        if (sparkClusterer != null) {
            // 大样本路径交给 Spark MLlib，由 driver 提交作业并让 executor 执行计算。
            LOGGER.info("AUTO 聚类选择 Spark KMeans 大样本路径，columnName={}，rowCount={}，exactLimit={}，targetClusterCount={}",
                    columnName, rows.size(), config.getMaxSampleCount(),
                    config.getTargetClusterCount());
            return sparkClusterer.cluster(columnName, dictionary, rows, config, randomSeed);
        }
        LOGGER.info("AUTO 聚类未获得 SparkSession，回退 Scalable 近似路径，columnName={}，rowCount={}，exactLimit={}，targetClusterCount={}",
                columnName, rows.size(), config.getMaxSampleCount(),
                config.getTargetClusterCount());
        return scalableClusterer.cluster(columnName, dictionary, rows, config, randomSeed);
    }
}
