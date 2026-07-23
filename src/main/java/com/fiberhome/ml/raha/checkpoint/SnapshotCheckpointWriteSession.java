package com.fiberhome.ml.raha.checkpoint;

import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存一次快照检查点分批写入所需的不可变全局上下文。
 */
public final class SnapshotCheckpointWriteSession {

    /** 检查点唯一标识。 */
    private final String checkpointId;
    /** 产生检查点的任务标识。 */
    private final String sourceJobId;
    /** 完整数据集元数据和数据帧。 */
    private final RahaDataset dataset;
    /** 输入快照。 */
    private final DatasetSnapshot snapshot;
    /** 全局策略计划。 */
    private final List<StrategyPlan> strategyPlans;
    /** 策略计划版本。 */
    private final String strategyPlanVersion;
    /** 执行配置指纹。 */
    private final String configFingerprint;
    /** 检查点创建时间。 */
    private final long createdAt;

    public SnapshotCheckpointWriteSession(
            String checkpointId,
            String sourceJobId,
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            List<StrategyPlan> strategyPlans,
            String strategyPlanVersion,
            String configFingerprint,
            long createdAt) {
        this.checkpointId = ValueUtils.requireNotBlank(checkpointId,
                "检查点标识");
        this.sourceJobId = ValueUtils.requireNotBlank(sourceJobId,
                "检查点来源任务标识");
        if (dataset == null || snapshot == null || strategyPlans == null
                || createdAt <= 0L) {
            throw new IllegalArgumentException("检查点写会话参数不能为空");
        }
        this.dataset = dataset;
        this.snapshot = snapshot;
        this.strategyPlans = Collections.unmodifiableList(
                new ArrayList<StrategyPlan>(strategyPlans));
        this.strategyPlanVersion = ValueUtils.requireNotBlank(
                strategyPlanVersion, "策略计划版本");
        this.configFingerprint = ValueUtils.requireNotBlank(
                configFingerprint, "执行配置指纹");
        this.createdAt = createdAt;
    }

    public String getCheckpointId() { return checkpointId; }
    public String getSourceJobId() { return sourceJobId; }
    public RahaDataset getDataset() { return dataset; }
    public DatasetSnapshot getSnapshot() { return snapshot; }
    public List<StrategyPlan> getStrategyPlans() { return strategyPlans; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public String getConfigFingerprint() { return configFingerprint; }
    public long getCreatedAt() { return createdAt; }
}
