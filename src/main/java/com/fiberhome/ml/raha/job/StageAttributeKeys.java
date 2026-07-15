package com.fiberhome.ml.raha.job;

/**
 * 约定任务阶段之间传递数据集和快照的属性键。
 */
public final class StageAttributeKeys {

    /** 已加载且只读的 Raha 数据集。 */
    public static final String RAHA_DATASET = "rahaDataset";
    /** 输入快照元数据。 */
    public static final String DATASET_SNAPSHOT = "datasetSnapshot";
    /** 当前快照生成的策略计划。 */
    public static final String STRATEGY_PLANS = "strategyPlans";
    /** 当前策略阶段的批量执行结果。 */
    public static final String STRATEGY_BATCH_RESULT = "strategyBatchResult";
    /** 当前策略阶段生成的候选命中。 */
    public static final String STRATEGY_HITS = "strategyHits";
    /** 当前任务生成的特征字典和稀疏向量。 */
    public static final String FEATURE_ASSEMBLY_RESULT = "featureAssemblyResult";
    /** 当前任务生成的列内聚类结果。 */
    public static final String CLUSTERING_BATCH_RESULT = "clusteringBatchResult";
    /** 当前任务生成或更新的待标注元组任务。 */
    public static final String ANNOTATION_TASKS = "annotationTasks";
    /** 当前任务采样阶段的评分、任务和指标。 */
    public static final String SAMPLING_BATCH_RESULT = "samplingBatchResult";
    /** 当前任务直接生成或接收的单元格标签。 */
    public static final String CELL_LABELS = "cellLabels";
    /** 当前任务生成的最终检测结果和指标。 */
    public static final String DETECTION_BATCH_RESULT = "detectionBatchResult";

    private StageAttributeKeys() {
    }
}
