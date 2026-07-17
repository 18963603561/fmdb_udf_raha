package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.api.DefaultRahaFacade;
import com.fiberhome.ml.raha.api.RahaFacade;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.RahaException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 表级入口的同步执行、日志和异常转换基类。
 */
abstract class AbstractRahaTableUdf extends UDF implements UDF1<String, String> {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRahaTableUdf.class);
    /** 固定业务类型。 */
    private final String operation;

    AbstractRahaTableUdf(String operation) {
        this.operation = operation;
    }

    @Override
    public final String call(String encodedRequest) {
        long startedAt = System.currentTimeMillis();
        try {
            // UDF 必须由驱动进程单次调用，直接取得当前活动 Spark 会话。
            SparkSession sparkSession = SparkSession.active();
            RahaFacade facade = DefaultRahaFacade.create(sparkSession);
            LOGGER.info("开始执行 Raha UDF，operation={}，requestLength={}", operation,
                    encodedRequest == null ? 0 : encodedRequest.length());
            String result = execute(facade, encodedRequest);
            LOGGER.info("Raha UDF 执行完成，operation={}，elapsedMillis={}", operation,
                    System.currentTimeMillis() - startedAt);
            return result;
        } catch (RahaException exception) {
            LOGGER.error("Raha UDF 业务失败，operation={}，errorCode={}", operation,
                    exception.getErrorCode(), exception);
            return failure(exception.getErrorCode().name(), exception.getMessage(),
                    System.currentTimeMillis() - startedAt);
        } catch (RuntimeException exception) {
            LOGGER.error("Raha UDF 执行失败，operation={}", operation, exception);
            return failure("UDF_EXECUTION_FAILED", exception.getClass().getSimpleName(),
                    System.currentTimeMillis() - startedAt);
        }
    }

    protected abstract String execute(RahaFacade facade, String encodedRequest);

    public final String evaluate(String encodedRequest) {
        return call(encodedRequest);
    }

    private static String failure(String errorCode, String message, long elapsedMillis) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("success", false);
        values.put("errorCode", errorCode);
        values.put("message", message);
        values.put("elapsedMillis", elapsedMillis);
        return JsonUtils.toJson(values);
    }
}
