package com.fiberhome.ml.raha.job.stage;

/**
 * 定义任务评估阶段需要执行的可替换评估逻辑。
 */
public interface StageEvaluator {

    /**
     * 使用当前阶段上下文生成评估结果。
     *
     * @param context 阶段执行上下文
     * @return 非空评估结果
     */
    Object evaluate(StageExecutionContext context);
}
