package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 单策略执行上下文，绑定只读数据集和不可变策略计划。
 */
public final class StrategyExecutionContext {

    /** 所属任务标识。 */
    private final String jobId;
    /** 当前阶段标识。 */
    private final String stageId;
    /** 只读输入数据集。 */
    private final RahaDataset dataset;
    /** 当前策略计划。 */
    private final StrategyPlan plan;

    public StrategyExecutionContext(String jobId,
                                    String stageId,
                                    RahaDataset dataset,
                                    StrategyPlan plan) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        this.stageId = ValueUtils.requireNotBlank(stageId, "阶段标识");
        if (dataset == null || plan == null) {
            throw new IllegalArgumentException("策略数据集和计划不能为空");
        }
        this.dataset = dataset;
        this.plan = plan;
    }

    public String getJobId() {
        return jobId;
    }

    public String getStageId() {
        return stageId;
    }

    public RahaDataset getDataset() {
        return dataset;
    }

    public StrategyPlan getPlan() {
        return plan;
    }

    /**
     * 返回当前单列策略的目标字段。
     *
     * @return 目标字段名称
     */
    public String getColumnName() {
        if (plan.getTargetColumns().size() != 1) {
            throw new IllegalStateException("当前基础策略必须只有一个目标字段");
        }
        return plan.getTargetColumns().get(0);
    }
}
