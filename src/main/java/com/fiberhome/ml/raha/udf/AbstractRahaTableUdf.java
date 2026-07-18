package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 提供表级 UDF 的公共日志和异常转换模板，具体入口保持强类型解析与直接执行。
 */
abstract class AbstractRahaTableUdf<T> extends UDF implements UDF1<String, String> {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRahaTableUdf.class);
    @Override
    public String call(String encodedRequest) {
        long startedAt = Math.max(1L, System.currentTimeMillis());
        RahaTaskType taskType = taskType();
        T request = null;
        String configVersion = null;
        try {
            request = parse(encodedRequest);
            RahaUdfCommonFields commonFields = commonFields(request);
            configVersion = HashUtils.sha256Hex(canonicalConfiguration(request));
            LOGGER.info("开始处理 Raha 表级 UDF，taskType={}，datasetId={}，caller={}，"
                            + "requestLength={}",
                    taskType, commonFields.getDatasetId(), commonFields.getCaller(),
                    encodedRequest == null ? 0 : encodedRequest.length());
            RahaUdfExecutionResult result = handle(request);
            if (result == null || result.getTaskType() != taskType
                    || !configVersion.equals(result.getConfigVersion())) {
                throw new IllegalStateException("UDF handler 返回了无效业务结果");
            }
            LOGGER.info("Raha 表级 UDF 同步执行完成，taskType={}，jobId={}，status={}，"
                            + "elapsedMillis={}",
                    taskType, result.getJobId(), result.getStatus(),
                    System.currentTimeMillis() - startedAt);
            return result.toJson();
        } catch (RahaUdfException exception) {
            LOGGER.warn("Raha 表级 UDF 请求被拒绝，taskType={}，errorCode={}，"
                            + "requestLength={}",
                    taskType, exception.getErrorCode(),
                    encodedRequest == null ? 0 : encodedRequest.length(), exception);
            return RahaUdfExecutionResult.rejected(taskType,
                    exception.getErrorCode(), exception.getMessage(), startedAt,
                    Math.max(startedAt, System.currentTimeMillis())).toJson();
        } catch (RuntimeException exception) {
            RahaUdfCommonFields commonFields = request == null
                    ? null : commonFields(request);
            LOGGER.error("Raha 表级 UDF 同步执行失败，taskType={}，jobId={}，"
                            + "datasetId={}，caller={}，resultTable={}，requestLength={}",
                    taskType,
                    commonFields == null ? null : commonFields.getIdempotencyKey(),
                    commonFields == null ? null : commonFields.getDatasetId(),
                    commonFields == null ? null : commonFields.getCaller(),
                    commonFields == null ? null : commonFields.getResultTable(),
                    encodedRequest == null ? 0 : encodedRequest.length(), exception);
            if (commonFields == null || configVersion == null) {
                return RahaUdfExecutionResult.rejected(taskType,
                        "UDF_EXECUTION_FAILED", exception.getClass().getSimpleName(),
                        startedAt, Math.max(startedAt, System.currentTimeMillis())).toJson();
            }
            return RahaUdfExecutionResult.failed(commonFields.getIdempotencyKey(),
                    taskType, configVersion, "UDF_EXECUTION_FAILED",
                    exception.getClass().getSimpleName(), startedAt,
                    Math.max(startedAt, System.currentTimeMillis())).toJson();
        }
    }

    /**
     * 提供 Hive 风格函数入口，支持通过 ADD JAR 和 CREATE FUNCTION 独立注册。
     *
     * @param encodedRequest 表单编码请求
     * @return 同步业务执行结果 JSON
     */
    public String evaluate(String encodedRequest) {
        return call(encodedRequest);
    }

    /** 返回当前入口固定的结果类型，仅用于日志和返回值。 */
    protected abstract RahaTaskType taskType();

    /** 使用当前入口专属解析方法生成强类型请求。 */
    protected abstract T parse(String encodedRequest);

    /** 返回当前强类型请求的公共字段。 */
    protected abstract RahaUdfCommonFields commonFields(T request);

    /** 返回当前请求用于计算配置版本的稳定文本。 */
    protected abstract String canonicalConfiguration(T request);

    /** 调用当前入口专属直接执行 handler。 */
    protected abstract RahaUdfExecutionResult handle(T request);
}
