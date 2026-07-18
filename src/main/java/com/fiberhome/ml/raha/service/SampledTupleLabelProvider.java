package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.sampling.AnnotationTask;

import java.util.List;

/**
 * 在主动采样选定元组后提供该行的直接人工标签。
 */
public interface SampledTupleLabelProvider {

    /**
     * 获取一个采样任务对应的直接标签。
     *
     * @param task 已选中的待标注任务
     * @return 当前行全部可检测字段的直接标签
     */
    List<CellLabel> labelsFor(AnnotationTask task);
}
