package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskResult;
import com.fiberhome.ml.raha.service.RahaTaskStatus;
import com.fiberhome.ml.raha.service.RahaTaskSummary;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.Map;

/**
 * 保存 UDF 同步执行状态、结果位置、摘要和脱敏错误信息。
 */
public final class RahaUdfExecutionResult {

    /** 本次执行标识，解析拒绝时允许为空。 */
    private final String jobId;
    /** 结果所属业务类型，仅用于返回和日志。 */
    private final RahaTaskType taskType;
    /** UDF 同步执行终态。 */
    private final RahaUdfExecutionStatus status;
    /** 业务结果逻辑位置，失败或拒绝时允许为空。 */
    private final String resultLocation;
    /** 本次请求的稳定配置版本。 */
    private final String configVersion;
    /** 不包含原始值的任务摘要。 */
    private final RahaTaskSummary summary;
    /** 失败或拒绝错误码。 */
    private final String errorCode;
    /** 脱敏错误摘要。 */
    private final String errorMessage;
    /** UDF 执行开始时间。 */
    private final long startedAt;
    /** UDF 执行结束时间。 */
    private final long finishedAt;

    private RahaUdfExecutionResult(String jobId,
                                   RahaTaskType taskType,
                                   RahaUdfExecutionStatus status,
                                   String resultLocation,
                                   String configVersion,
                                   RahaTaskSummary summary,
                                   String errorCode,
                                   String errorMessage,
                                   long startedAt,
                                   long finishedAt) {
        if (taskType == null || status == null || startedAt <= 0L
                || finishedAt < startedAt) {
            throw new IllegalArgumentException("UDF 执行结果类型、状态和时间必须有效");
        }
        if (status == RahaUdfExecutionStatus.REJECTED) {
            ValueUtils.requireNotBlank(errorCode, "UDF 拒绝错误码");
            ValueUtils.requireNotBlank(errorMessage, "UDF 拒绝错误摘要");
        } else {
            ValueUtils.requireNotBlank(jobId, "UDF 执行标识");
            ValueUtils.requireNotBlank(configVersion, "UDF 配置版本");
        }
        if ((status == RahaUdfExecutionStatus.SUCCEEDED
                || status == RahaUdfExecutionStatus.PARTIAL_SUCCESS)
                && (resultLocation == null || resultLocation.trim().isEmpty())) {
            throw new IllegalArgumentException("成功执行结果必须包含结果位置");
        }
        if (status == RahaUdfExecutionStatus.SUCCEEDED
                && (errorCode != null || errorMessage != null)) {
            throw new IllegalArgumentException("成功执行结果不能包含错误信息");
        }
        this.jobId = jobId;
        this.taskType = taskType;
        this.status = status;
        this.resultLocation = resultLocation;
        this.configVersion = configVersion;
        this.summary = summary;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
    }

    /**
     * 将强类型服务结果转换为 UDF 同步执行结果。
     *
     * @param taskResult 采样、训练或检测服务结果
     * @param configVersion UDF 请求配置版本
     * @return 对齐服务终态的 UDF 结果
     */
    public static RahaUdfExecutionResult fromTaskResult(
            RahaTaskResult<?> taskResult,
            String configVersion) {
        if (taskResult == null) {
            throw new IllegalArgumentException("服务任务结果不能为空");
        }
        RahaTaskSummary summary = taskResult.getSummary();
        return new RahaUdfExecutionResult(taskResult.getTaskId(),
                taskResult.getTaskType(), executionStatus(taskResult.getStatus()),
                taskResult.getResultLocation(), configVersion, summary,
                taskResult.getErrorCode(), taskResult.getErrorMessage(),
                summary.getStartedAt(), summary.getCompletedAt());
    }

    /**
     * 创建已完成的采样 UDF 结果。
     */
    public static RahaUdfExecutionResult completed(String jobId,
                                                    RahaTaskType taskType,
                                                    String resultLocation,
                                                    String configVersion,
                                                    RahaTaskSummary summary) {
        if (summary == null) {
            throw new IllegalArgumentException("UDF 完成摘要不能为空");
        }
        return new RahaUdfExecutionResult(jobId, taskType,
                RahaUdfExecutionStatus.SUCCEEDED, resultLocation,
                configVersion, summary, null, null,
                summary.getStartedAt(), summary.getCompletedAt());
    }

    /**
     * 创建请求解析拒绝结果。
     */
    public static RahaUdfExecutionResult rejected(RahaTaskType taskType,
                                                   String errorCode,
                                                   String errorMessage,
                                                   long startedAt,
                                                   long finishedAt) {
        return new RahaUdfExecutionResult(null, taskType,
                RahaUdfExecutionStatus.REJECTED, null, null, null,
                errorCode, errorMessage, startedAt, finishedAt);
    }

    /**
     * 创建未返回服务结果的业务执行失败结果。
     */
    public static RahaUdfExecutionResult failed(String jobId,
                                                 RahaTaskType taskType,
                                                 String configVersion,
                                                 String errorCode,
                                                 String errorMessage,
                                                 long startedAt,
                                                 long finishedAt) {
        return new RahaUdfExecutionResult(jobId, taskType,
                RahaUdfExecutionStatus.FAILED, null, configVersion, null,
                errorCode, errorMessage, startedAt, finishedAt);
    }

    /**
     * 返回不包含原始输入值的稳定 JSON 文本。
     */
    public String toJson() {
        return "{" + json("jobId", jobId) + ","
                + json("taskType", taskType.name()) + ","
                + json("status", status.name()) + ","
                + json("resultLocation", resultLocation) + ","
                + json("configVersion", configVersion) + ","
                + "\"summary\":" + summaryJson(summary) + ","
                + json("errorCode", errorCode) + ","
                + json("errorMessage", errorMessage) + ","
                + "\"startedAt\":" + startedAt + ","
                + "\"finishedAt\":" + finishedAt + "}";
    }

    private static RahaUdfExecutionStatus executionStatus(RahaTaskStatus status) {
        if (status == RahaTaskStatus.SUCCEEDED) {
            return RahaUdfExecutionStatus.SUCCEEDED;
        }
        if (status == RahaTaskStatus.PARTIAL_SUCCESS) {
            return RahaUdfExecutionStatus.PARTIAL_SUCCESS;
        }
        return RahaUdfExecutionStatus.FAILED;
    }

    private static String summaryJson(RahaTaskSummary value) {
        if (value == null) {
            return "null";
        }
        return "{\"totalCount\":" + value.getTotalCount()
                + ",\"successfulCount\":" + value.getSuccessfulCount()
                + ",\"skippedCount\":" + value.getSkippedCount()
                + ",\"failedCount\":" + value.getFailedCount()
                + ",\"elapsedMillis\":" + value.getElapsedMillis()
                + ",\"details\":" + mapJson(value.getDetails()) + "}";
    }

    private static String mapJson(Map<String, String> values) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (!first) {
                json.append(',');
            }
            json.append(json(entry.getKey(), entry.getValue()));
            first = false;
        }
        return json.append('}').toString();
    }

    private static String json(String name, String value) {
        return "\"" + escape(name) + "\":"
                + (value == null ? "null" : "\"" + escape(value) + "\"");
    }

    private static String escape(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '"' || character == '\\') {
                escaped.append('\\').append(character);
            } else if (character == '\n') {
                escaped.append("\\n");
            } else if (character == '\r') {
                escaped.append("\\r");
            } else if (character == '\t') {
                escaped.append("\\t");
            } else if (character < 0x20) {
                escaped.append(String.format("\\u%04x", (int) character));
            } else {
                escaped.append(character);
            }
        }
        return escaped.toString();
    }

    public String getJobId() { return jobId; }
    public RahaTaskType getTaskType() { return taskType; }
    public RahaUdfExecutionStatus getStatus() { return status; }
    public String getResultLocation() { return resultLocation; }
    public String getConfigVersion() { return configVersion; }
    public RahaTaskSummary getSummary() { return summary; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public long getStartedAt() { return startedAt; }
    public long getFinishedAt() { return finishedAt; }
}
