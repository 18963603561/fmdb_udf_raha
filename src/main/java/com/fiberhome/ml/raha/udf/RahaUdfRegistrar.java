package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.UdfConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在 FMDB Spark 会话中统一注册三个异步表级 Raha UDF。
 */
public final class RahaUdfRegistrar {

    /** 训练函数固定名称。 */
    public static final String TRAIN_FUNCTION = RahaDefaultConfigProvider.factory()
            .udfConfig().getTrainFunction();
    /** 检测函数固定名称。 */
    public static final String DETECT_FUNCTION = RahaDefaultConfigProvider.factory()
            .udfConfig().getDetectFunction();
    /** 采样函数固定名称。 */
    public static final String SAMPLE_FUNCTION = RahaDefaultConfigProvider.factory()
            .udfConfig().getSampleFunction();
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaUdfRegistrar.class);
    /** 当前注册使用的 UDF 名称配置。 */
    private final UdfConfig config;

    public RahaUdfRegistrar() {
        this(RahaDefaultConfigProvider.factory().udfConfig());
    }

    public RahaUdfRegistrar(UdfConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("UDF 注册配置不能为空");
        }
        this.config = config;
    }

    /**
     * 配置当前进程提交器并注册三个字符串入参、JSON 文本返回的函数。
     */
    public void register(SparkSession sparkSession, RahaUdfJobSubmitter submitter) {
        if (sparkSession == null || submitter == null) {
            throw new IllegalArgumentException("UDF 注册会话和提交器不能为空");
        }
        RahaUdfRuntime.configure(submitter);
        LOGGER.info("开始注册 Raha 表级 UDF");
        try {
            sparkSession.udf().register(config.getTrainFunction(),
                    new F_DW_RAHATRAIN(), DataTypes.StringType);
            sparkSession.udf().register(config.getDetectFunction(),
                    new F_DW_RAHADETECT(), DataTypes.StringType);
            sparkSession.udf().register(config.getSampleFunction(),
                    new F_DW_RAHASAMPLE(), DataTypes.StringType);
            LOGGER.info("Raha 表级 UDF 注册完成，functions={},{},{}",
                    config.getTrainFunction(), config.getDetectFunction(),
                    config.getSampleFunction());
        } catch (RuntimeException exception) {
            // Spark 或 FMDB 函数注册失败时清理提交器，避免保留不可用运行时。
            RahaUdfRuntime.clear();
            LOGGER.error("Raha 表级 UDF 注册失败", exception);
            throw exception;
        }
    }
}
