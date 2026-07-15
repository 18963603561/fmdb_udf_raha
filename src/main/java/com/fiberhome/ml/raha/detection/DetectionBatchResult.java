package com.fiberhome.ml.raha.detection;

import com.fiberhome.ml.raha.data.DetectionResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存基础检测结果和任务级检测指标。
 */
public final class DetectionBatchResult {

    /** 单元格检测结果。 */
    private final List<DetectionResult> results;
    /** 检测指标。 */
    private final DetectionMetrics metrics;

    public DetectionBatchResult(List<DetectionResult> results, DetectionMetrics metrics) {
        if (results == null || metrics == null) {
            throw new IllegalArgumentException("检测结果和指标不能为空");
        }
        this.results = Collections.unmodifiableList(new ArrayList<DetectionResult>(results));
        this.metrics = metrics;
    }

    public List<DetectionResult> getResults() { return results; }
    public DetectionMetrics getMetrics() { return metrics; }
}
