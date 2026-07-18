package com.fiberhome.ml.raha.job.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 返回任务最终状态、全部阶段尝试和阶段共享结果。
 */
public final class JobRunResult {

    /** 最终任务快照。 */
    private final RahaJob job;
    /** 本次运行产生的阶段尝试。 */
    private final List<RahaStage> stages;
    /** 阶段共享结果快照。 */
    private final Map<String, Object> attributes;

    public JobRunResult(RahaJob job, List<RahaStage> stages, Map<String, Object> attributes) {
        if (job == null || stages == null || attributes == null) {
            throw new IllegalArgumentException("任务运行结果参数不能为空");
        }
        this.job = job.snapshot();
        List<RahaStage> stageCopies = new ArrayList<RahaStage>(stages.size());
        for (RahaStage stage : stages) {
            stageCopies.add(stage.snapshot());
        }
        this.stages = Collections.unmodifiableList(stageCopies);
        this.attributes = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(attributes));
    }

    public RahaJob getJob() {
        return job.snapshot();
    }

    public List<RahaStage> getStages() {
        return stages;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }
}

