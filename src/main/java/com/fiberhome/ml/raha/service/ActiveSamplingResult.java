package com.fiberhome.ml.raha.service;

import com.fiberhome.ml.raha.cluster.ClusteringBatchResult;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.sampling.AnnotationTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 保存逐轮主动采样产生的任务、行顺序、直接标签和最后一轮聚类结果。
 */
public final class ActiveSamplingResult {

    /** 按轮次保存的已完成标注任务。 */
    private final List<AnnotationTask> tasks;
    /** 按采样顺序保存的稳定行标识。 */
    private final List<String> rowIds;
    /** 主动采样累计得到的直接标签。 */
    private final List<CellLabel> labels;
    /** 直接标签单元格标识。 */
    private final Set<String> cellIds;
    /** 最后一轮列内聚类结果。 */
    private final ClusteringBatchResult clustering;

    public ActiveSamplingResult(List<AnnotationTask> tasks,
                                List<String> rowIds,
                                List<CellLabel> labels,
                                Set<String> cellIds,
                                ClusteringBatchResult clustering) {
        if (tasks == null || rowIds == null || labels == null
                || cellIds == null || clustering == null) {
            throw new IllegalArgumentException("主动采样结果不能为空");
        }
        this.tasks = Collections.unmodifiableList(
                new ArrayList<AnnotationTask>(tasks));
        this.rowIds = Collections.unmodifiableList(new ArrayList<String>(rowIds));
        this.labels = Collections.unmodifiableList(new ArrayList<CellLabel>(labels));
        this.cellIds = Collections.unmodifiableSet(new LinkedHashSet<String>(cellIds));
        this.clustering = clustering;
    }

    public List<AnnotationTask> getTasks() { return tasks; }
    public List<String> getRowIds() { return rowIds; }
    public List<CellLabel> getLabels() { return labels; }
    public Set<String> getCellIds() { return cellIds; }
    public ClusteringBatchResult getClustering() { return clustering; }
}
