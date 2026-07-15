package com.fiberhome.ml.raha.checkpoint;

/**
 * 定义一个支持阶段检查点审计和重试的业务任务。
 *
 * @param <T> 成功结果载荷类型
 */
@FunctionalInterface
public interface CheckpointTask<T> {

    /**
     * 执行指定序号的阶段尝试。
     *
     * @param attemptId 全局递增的阶段尝试序号
     * @return 本次尝试的成功或失败结果
     */
    CheckpointTaskResult<T> execute(int attemptId);
}
