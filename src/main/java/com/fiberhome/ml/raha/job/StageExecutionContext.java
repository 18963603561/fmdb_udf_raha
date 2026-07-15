package com.fiberhome.ml.raha.job;

import com.fiberhome.ml.raha.config.RahaJobConfig;

import java.util.Map;

/**
 * 阶段处理器执行上下文，同一任务的属性映射用于传递只读或新版本阶段结果。
 */
public final class StageExecutionContext {

    /** 当前任务快照。 */
    private final RahaJob job;
    /** 当前任务配置。 */
    private final RahaJobConfig config;
    /** 当前阶段快照。 */
    private final RahaStage stage;
    /** 任务内阶段共享属性，写入时必须使用新的键或不可变对象。 */
    private final Map<String, Object> attributes;

    public StageExecutionContext(RahaJob job,
                                 RahaJobConfig config,
                                 RahaStage stage,
                                 Map<String, Object> attributes) {
        if (job == null || config == null || stage == null || attributes == null) {
            throw new IllegalArgumentException("阶段执行上下文参数不能为空");
        }
        this.job = job;
        this.config = config;
        this.stage = stage;
        this.attributes = attributes;
    }

    public RahaJob getJob() {
        return job;
    }

    public RahaJobConfig getConfig() {
        return config;
    }

    public RahaStage getStage() {
        return stage;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }
}

