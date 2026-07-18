package com.fiberhome.ml.raha.strategy.api;

import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionContext;
import com.fiberhome.ml.raha.strategy.plan.StrategyCandidate;
import java.util.List;

/**
 * 基础检测策略统一契约，只输出候选命中，不输出最终错误判断。
 */
public interface DetectionStrategy {

    /**
     * 返回实现类型，用于策略计划分发。
     *
     * @return 策略类型
     */
    String getStrategyType();

    /**
     * 返回策略族，用于运行摘要和统计。
     *
     * @return 策略族
     */
    StrategyFamily getStrategyFamily();

    /**
     * 执行策略并返回候选命中。
     *
     * @param context 策略执行上下文
     * @return 候选命中列表
     */
    List<StrategyCandidate> detect(StrategyExecutionContext context);
}
