package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.domain.RahaStage;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
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

    private RahaTaskExecutionResult(JobRunResult runResult, boolean reused) {
        if (runResult == null) {
            throw new IllegalArgumentException("任务运行结果不能为空");
        }
        this.runResult = runResult;
        this.reused = reused;
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
        this.resultLocation = location instanceof String ? (String) location : null;
    }

    static RahaTaskExecutionResult executed(JobRunResult runResult) {
        return new RahaTaskExecutionResult(runResult, false);
    }

    static RahaTaskExecutionResult reused(RahaJob job, List<RahaStage> stages) {
        return new RahaTaskExecutionResult(new JobRunResult(job, stages,
                java.util.Collections.<String, Object>emptyMap()), true);
    }

    public RahaJob getJob() { return runResult.getJob(); }
    public List<RahaStage> getStages() { return runResult.getStages(); }
    public Map<String, Object> getAttributes() { return runResult.getAttributes(); }
    public JobRunResult getRunResult() { return runResult; }
    public boolean isReused() { return reused; }
    public Object getPayload() { return payload; }
    public String getResultLocation() { return resultLocation; }

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
