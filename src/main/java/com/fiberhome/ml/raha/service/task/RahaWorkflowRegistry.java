package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.type.JobType;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * 按任务类型保存唯一工作流，禁止重复注册和运行期静默替换。
 */
public final class RahaWorkflowRegistry {

    /** 任务类型到工作流的只读映射。 */
    private final Map<JobType, RahaWorkflow> workflows;

    public RahaWorkflowRegistry(Collection<RahaWorkflow> workflows) {
        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("工作流集合不能为空");
        }
        EnumMap<JobType, RahaWorkflow> values = new EnumMap<JobType, RahaWorkflow>(JobType.class);
        for (RahaWorkflow workflow : workflows) {
            if (workflow == null || workflow.getJobType() == null) {
                throw new IllegalArgumentException("工作流和任务类型不能为空");
            }
            if (values.put(workflow.getJobType(), workflow) != null) {
                throw new IllegalArgumentException("任务类型重复注册工作流："
                        + workflow.getJobType());
            }
        }
        this.workflows = Collections.unmodifiableMap(values);
    }

    public RahaWorkflow require(JobType jobType) {
        RahaWorkflow workflow = workflows.get(jobType);
        if (workflow == null) {
            throw new IllegalArgumentException("没有注册任务工作流：" + jobType);
        }
        return workflow;
    }
}
