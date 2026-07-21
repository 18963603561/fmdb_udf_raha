package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.label.CellLabel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存最小采样入口允许调用方覆盖的非必填参数。
 */
public final class SamplingRequestOptions {

    /** 可选标注预算，为空时读取统一运行配置。 */
    private final Integer labelingBudget;
    /** 当前采样轮次，默认从第一轮开始。 */
    private final int samplingRound;
    /** 已经持有的直接标签，用于排除或复用已标注单元格。 */
    private final List<CellLabel> existingLabels;
    /** 执行覆盖选项，用于控制强制运行和请求级幂等。 */
    private final ExecutionOverrideOptions executionOverrideOptions;

    public SamplingRequestOptions(Integer labelingBudget,
                                  int samplingRound,
                                  List<CellLabel> existingLabels) {
        this(labelingBudget, samplingRound, existingLabels,
                ExecutionOverrideOptions.DEFAULT);
    }

    public SamplingRequestOptions(Integer labelingBudget,
                                  int samplingRound,
                                  List<CellLabel> existingLabels,
                                  ExecutionOverrideOptions executionOverrideOptions) {
        if (labelingBudget != null && labelingBudget <= 0) {
            throw new IllegalArgumentException("采样标注预算必须大于零");
        }
        if (samplingRound <= 0) {
            throw new IllegalArgumentException("采样轮次必须大于零");
        }
        this.labelingBudget = labelingBudget;
        this.samplingRound = samplingRound;
        this.existingLabels = immutableLabels(existingLabels);
        this.executionOverrideOptions = executionOverrideOptions == null
                ? ExecutionOverrideOptions.DEFAULT : executionOverrideOptions;
    }

    /**
     * 创建使用运行配置预算、第一轮和空标签集合的选项。
     *
     * @return 默认采样选项
     */
    public static SamplingRequestOptions defaults() {
        return new SamplingRequestOptions(null, 1,
                Collections.<CellLabel>emptyList());
    }

    public Integer getLabelingBudget() { return labelingBudget; }
    public int getSamplingRound() { return samplingRound; }
    public List<CellLabel> getExistingLabels() { return existingLabels; }
    public ExecutionOverrideOptions getExecutionOverrideOptions() {
        return executionOverrideOptions;
    }

    private static List<CellLabel> immutableLabels(List<CellLabel> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<CellLabel> result = new ArrayList<CellLabel>(values.size());
        for (CellLabel value : values) {
            if (value == null) {
                throw new IllegalArgumentException("采样已有标签不能包含空值");
            }
            result.add(value);
        }
        return Collections.unmodifiableList(result);
    }
}
