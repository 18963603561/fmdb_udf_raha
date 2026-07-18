package com.fiberhome.ml.raha.udf;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在 FMDB Spark 会话中分别注册三个同步表级 Raha UDF。
 */
public final class RahaUdfRegistrar {

    /** 训练函数固定名称。 */
    public static final String TRAIN_FUNCTION = "F_DW_RAHATRAIN";
    /** 检测函数固定名称。 */
    public static final String DETECT_FUNCTION = "F_DW_RAHADETECT";
    /** 采样函数固定名称。 */
    public static final String SAMPLE_FUNCTION = "F_DW_RAHASAMPLE";
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaUdfRegistrar.class);

    /**
     * 注册三个字符串入参、JSON 文本返回的同步执行函数。
     */
    public void register(SparkSession sparkSession,
                         RahaSampleUdfHandler sampleHandler,
                         RahaTrainUdfHandler trainHandler,
                         RahaDetectUdfHandler detectHandler) {
        if (sparkSession == null || sampleHandler == null
                || trainHandler == null || detectHandler == null) {
            throw new IllegalArgumentException("UDF 注册会话和三个 handler 不能为空");
        }
        LOGGER.info("开始注册 Raha 表级 UDF");
        try {
            sparkSession.udf().register(TRAIN_FUNCTION,
                    new F_DW_RAHATRAIN(trainHandler), DataTypes.StringType);
            sparkSession.udf().register(DETECT_FUNCTION,
                    new F_DW_RAHADETECT(detectHandler), DataTypes.StringType);
            sparkSession.udf().register(SAMPLE_FUNCTION,
                    new F_DW_RAHASAMPLE(sampleHandler), DataTypes.StringType);
            LOGGER.info("Raha 表级 UDF 注册完成，functions={},{},{}",
                    TRAIN_FUNCTION, DETECT_FUNCTION, SAMPLE_FUNCTION);
        } catch (RuntimeException exception) {
            LOGGER.error("Raha 表级 UDF 注册失败", exception);
            throw exception;
        }
    }
}
