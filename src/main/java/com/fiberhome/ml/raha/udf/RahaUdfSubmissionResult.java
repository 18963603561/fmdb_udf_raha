package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 表级 UDF 返回的异步任务标识、状态、位置和失败追溯信息。
 */
public final class RahaUdfSubmissionResult {

    /** 任务标识，拒绝且未创建任务时为空。 */
    private final String jobId;
    /** Raha 服务任务类型。 */
    private final RahaTaskType taskType;
    /** UDF 提交状态。 */
    private final RahaUdfSubmissionStatus status;
    /** 预期任务结果位置。 */
    private final String resultLocation;
    /** 请求配置版本。 */
    private final String configVersion;
    /** 失败错误码。 */
    private final String errorCode;
    /** 不包含输入值的失败摘要。 */
    private final String errorMessage;
    /** 提交响应时间。 */
    private final long submittedAt;

    private RahaUdfSubmissionResult(String jobId,
                                    RahaTaskType taskType,
                                    RahaUdfSubmissionStatus status,
                                    String resultLocation,
                                    String configVersion,
                                    String errorCode,
                                    String errorMessage,
                                    long submittedAt) {
        if (taskType == null || status == null || submittedAt <= 0L) {
            throw new IllegalArgumentException("UDF 提交结果类型、状态和时间必须有效");
        }
        if (status == RahaUdfSubmissionStatus.REJECTED) {
            ValueUtils.requireNotBlank(errorCode, "UDF 拒绝错误码");
            ValueUtils.requireNotBlank(errorMessage, "UDF 拒绝错误摘要");
        } else {
            ValueUtils.requireNotBlank(jobId, "UDF 已接受任务标识");
            ValueUtils.requireNotBlank(resultLocation, "UDF 任务结果位置");
            ValueUtils.requireNotBlank(configVersion, "UDF 任务配置版本");
            if (errorCode != null || errorMessage != null) {
                throw new IllegalArgumentException("UDF 已接受结果不能包含错误信息");
            }
        }
        this.jobId = jobId;
        this.taskType = taskType;
        this.status = status;
        this.resultLocation = resultLocation;
        this.configVersion = configVersion;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.submittedAt = submittedAt;
    }

    public static RahaUdfSubmissionResult accepted(String jobId,
                                                     RahaTaskType taskType,
                                                     String resultLocation,
                                                     String configVersion,
                                                     long submittedAt) {
        return new RahaUdfSubmissionResult(jobId, taskType,
                RahaUdfSubmissionStatus.ACCEPTED, resultLocation, configVersion,
                null, null, submittedAt);
    }

    public static RahaUdfSubmissionResult duplicate(String jobId,
                                                      RahaTaskType taskType,
                                                      String resultLocation,
                                                      String configVersion,
                                                      long submittedAt) {
        return new RahaUdfSubmissionResult(jobId, taskType,
                RahaUdfSubmissionStatus.DUPLICATE, resultLocation, configVersion,
                null, null, submittedAt);
    }

    public static RahaUdfSubmissionResult rejected(RahaTaskType taskType,
                                                     String errorCode,
                                                     String errorMessage,
                                                     long submittedAt) {
        return new RahaUdfSubmissionResult(null, taskType,
                RahaUdfSubmissionStatus.REJECTED, null, null,
                errorCode, errorMessage, submittedAt);
    }

    /**
     * 返回不包含原始输入数据的稳定 JSON 文本。
     */
    public String toJson() {
        return "{" + json("jobId", jobId) + ","
                + json("taskType", taskType.name()) + ","
                + json("status", status.name()) + ","
                + json("resultLocation", resultLocation) + ","
                + json("configVersion", configVersion) + ","
                + json("errorCode", errorCode) + ","
                + json("errorMessage", errorMessage) + ","
                + "\"submittedAt\":" + submittedAt + "}";
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
    public RahaUdfSubmissionStatus getStatus() { return status; }
    public String getResultLocation() { return resultLocation; }
    public String getConfigVersion() { return configVersion; }
    public String getErrorCode() { return errorCode; }
    public String getErrorMessage() { return errorMessage; }
    public long getSubmittedAt() { return submittedAt; }
}
