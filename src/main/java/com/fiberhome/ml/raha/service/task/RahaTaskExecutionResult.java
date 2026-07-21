package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.domain.RahaStage;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 统一返回任务状态、阶段轨迹、业务输出和结果位置。
 */
public final class RahaTaskExecutionResult {

    /** 任务执行内核结果。 */
    private final JobRunResult runResult;
    /** 是否复用了先前提交的同幂等任务。 */
    private final boolean reused;
    /** 训练、检测或采样类型化输出。 */
    private final Object payload;
    /** 最终结果逻辑位置。 */
    private final String resultLocation;
    /** 可持久化的轻量结果摘要。 */
    private final Map<String, Object> resultSummary;

    private RahaTaskExecutionResult(JobRunResult runResult, boolean reused) {
        this(runResult, reused, Collections.<String, Object>emptyMap());
    }

    private RahaTaskExecutionResult(JobRunResult runResult,
                                    boolean reused,
                                    Map<String, Object> resultSummary) {
        if (runResult == null) {
            throw new IllegalArgumentException("任务运行结果不能为空");
        }
        this.runResult = runResult;
        this.reused = reused;
        this.resultSummary = resultSummary == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(resultSummary));
        Map<String, Object> attributes = runResult.getAttributes();
        Object value = attributes.get(StageAttributeKeys.TRAIN_OUTPUT);
        if (value == null) {
            value = attributes.get(StageAttributeKeys.DETECT_OUTPUT);
        }
        if (value == null) {
            value = attributes.get(StageAttributeKeys.SAMPLE_OUTPUT);
        }
        this.payload = value;
        Object location = attributes.get(StageAttributeKeys.RESULT_LOCATION);
        if (location instanceof String) {
            this.resultLocation = (String) location;
        } else {
            Object summaryLocation = this.resultSummary.get("resultLocation");
            this.resultLocation = summaryLocation instanceof String
                    ? (String) summaryLocation : null;
        }
    }

    static RahaTaskExecutionResult executed(JobRunResult runResult) {
        return new RahaTaskExecutionResult(runResult, false);
    }

    static RahaTaskExecutionResult executed(JobRunResult runResult,
                                            Map<String, Object> resultSummary) {
        return new RahaTaskExecutionResult(runResult, false, resultSummary);
    }

    static RahaTaskExecutionResult reused(RahaJob job, List<RahaStage> stages) {
        return new RahaTaskExecutionResult(new JobRunResult(job, stages,
                java.util.Collections.<String, Object>emptyMap()), true);
    }

    static RahaTaskExecutionResult reused(RahaJob job,
                                          List<RahaStage> stages,
                                          RahaTaskExecutionRequest request,
                                          Map<String, Object> resultSummary) {
        Map<String, Object> attributes = new LinkedHashMap<String, Object>();
        if (request != null) {
            attributes.putAll(request.executionSummarySeed());
        }
        if (resultSummary != null) {
            attributes.putAll(resultSummary);
            Object location = resultSummary.get("resultLocation");
            if (location instanceof String) {
                attributes.put(StageAttributeKeys.RESULT_LOCATION, location);
            }
        }
        return new RahaTaskExecutionResult(new JobRunResult(job, stages,
                attributes), true, attributes);
    }

    public RahaJob getJob() { return runResult.getJob(); }
    public List<RahaStage> getStages() { return runResult.getStages(); }
    public Map<String, Object> getAttributes() { return runResult.getAttributes(); }
    public JobRunResult getRunResult() { return runResult; }
    public boolean isReused() { return reused; }
    public Object getPayload() { return payload; }
    public String getResultLocation() { return resultLocation; }
    public Map<String, Object> getResultSummary() { return resultSummary; }

    public <T> T getPayload(Class<T> payloadType) {
        if (payloadType == null) {
            throw new IllegalArgumentException("业务输出类型不能为空");
        }
        if (payload == null) {
            return null;
        }
        if (!payloadType.isInstance(payload)) {
            throw new IllegalArgumentException("业务输出类型不匹配：" + payloadType.getName());
        }
        return payloadType.cast(payload);
    }
}
