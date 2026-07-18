package com.fiberhome.ml.raha.job.stage;

import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.detection.service.DetectionBatchResult;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.sampling.service.SamplingBatchResult;
import com.fiberhome.ml.raha.strategy.execution.StrategyBatchResult;

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
    /** 当前快照策略计划的稳定版本。 */
    public static final String STRATEGY_PLAN_VERSION = "strategyPlanVersion";
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
    /** 当前任务标签传播阶段结果。 */
    public static final String LABEL_PROPAGATION_RESULT = "labelPropagationResult";
    /** 当前任务训练服务结果。 */
    public static final String TRAIN_SERVICE_RESULT = "trainServiceResult";
    /** 当前任务训练业务输出。 */
    public static final String TRAIN_OUTPUT = "trainOutput";
    /** 当前任务已发布模型检测服务结果。 */
    public static final String DETECT_SERVICE_RESULT = "detectServiceResult";
    /** 当前任务已发布模型检测业务输出。 */
    public static final String DETECT_OUTPUT = "detectOutput";
    /** 当前任务采样服务结果。 */
    public static final String SAMPLE_SERVICE_RESULT = "sampleServiceResult";
    /** 当前任务采样业务输出。 */
    public static final String SAMPLE_OUTPUT = "sampleOutput";
    /** 当前任务模型或检测评估结果。 */
    public static final String EVALUATION_RESULT = "evaluationResult";
    /** 当前任务最终结果逻辑位置。 */
    public static final String RESULT_LOCATION = "resultLocation";
    /** 当前任务生成的最终检测结果和指标。 */
    public static final String DETECTION_BATCH_RESULT = "detectionBatchResult";

    private StageAttributeKeys() {
    }
}
