package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.data.type.StageType;

/**
 * 调用可替换评估器生成模型或检测评估结果。
 */
public final class EvaluationStageHandler implements StageHandler {

    /** 当前工作流使用的评估器。 */
    private final StageEvaluator evaluator;

    public EvaluationStageHandler(StageEvaluator evaluator) {
        if (evaluator == null) {
            throw new IllegalArgumentException("阶段评估器不能为空");
        }
        this.evaluator = evaluator;
    }

    @Override
    public StageType getStageType() {
        return StageType.EVALUATE;
    }

    @Override
    public StageResult execute(StageExecutionContext context) {
        Object result = evaluator.evaluate(context);
        if (result == null) {
            return StageResult.failure("EVALUATION_RESULT_REQUIRED",
                    "评估阶段没有生成结果", false, 0L, 0L);
        }
        context.getAttributes().put(StageAttributeKeys.EVALUATION_RESULT, result);
        return StageResult.success();
    }
}
