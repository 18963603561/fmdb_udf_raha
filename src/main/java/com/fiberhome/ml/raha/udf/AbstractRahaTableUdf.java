package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;
import org.apache.spark.sql.api.java.UDF1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统一表级 UDF 参数解析、异步提交、错误转换和安全日志行为。
 */
abstract class AbstractRahaTableUdf implements UDF1<String, String> {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRahaTableUdf.class);
    /** 当前 UDF 固定任务类型。 */
    private final RahaTaskType taskType;
    /** 请求解析器。 */
    private final RahaUdfRequestParser parser;
    /** 可序列化部署实现或测试注入的任务提交器。 */
    private final RahaUdfJobSubmitter submitter;

    AbstractRahaTableUdf(RahaTaskType taskType,
                         RahaUdfRequestParser parser,
                         RahaUdfJobSubmitter submitter) {
        if (taskType == null || parser == null || submitter == null) {
            throw new IllegalArgumentException("表级 UDF 依赖不能为空");
        }
        this.taskType = taskType;
        this.parser = parser;
        this.submitter = submitter;
    }

    @Override
    public String call(String encodedRequest) {
        long startedAt = Math.max(1L, System.currentTimeMillis());
        try {
            RahaUdfRequest request = parser.parse(taskType, encodedRequest);
            LOGGER.info("开始处理 Raha 表级 UDF，taskType={}，datasetId={}，caller={}，"
                            + "requestLength={}",
                    taskType, request.getDatasetId(), request.getCaller(),
                    encodedRequest == null ? 0 : encodedRequest.length());
            RahaUdfSubmissionResult result = submitter.submit(request);
            LOGGER.info("Raha 表级 UDF 处理完成，taskType={}，jobId={}，status={}，"
                            + "elapsedMillis={}",
                    taskType, result.getJobId(), result.getStatus(),
                    System.currentTimeMillis() - startedAt);
            return result.toJson();
        } catch (RahaUdfException exception) {
            LOGGER.warn("Raha 表级 UDF 请求被拒绝，taskType={}，errorCode={}，"
                            + "requestLength={}",
                    taskType, exception.getErrorCode(),
                    encodedRequest == null ? 0 : encodedRequest.length(), exception);
            return RahaUdfSubmissionResult.rejected(taskType,
                    exception.getErrorCode(), exception.getMessage(), startedAt).toJson();
        } catch (RuntimeException exception) {
            // 外部 FMDB 写入或任务仓储异常必须返回可追溯失败并记录完整堆栈。
            LOGGER.error("Raha 表级 UDF 提交失败，taskType={}，requestLength={}",
                    taskType, encodedRequest == null ? 0 : encodedRequest.length(), exception);
            return RahaUdfSubmissionResult.rejected(taskType,
                    "UDF_SUBMISSION_FAILED", exception.getClass().getSimpleName(),
                    startedAt).toJson();
        }
    }
}
