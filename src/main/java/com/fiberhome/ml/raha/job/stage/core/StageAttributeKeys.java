package com.fiberhome.ml.raha.job.stage.core;

/**
 * 约定任务阶段之间传递中间产物的上下文属性键。
 *
 * <p>常量按流水线生产顺序排列，便于定位每个阶段写入和后续阶段读取的共享数据。</p>
 */
public final class StageAttributeKeys {

    /** 步骤 1：数据加载阶段写入，表示已加载且只读的 Raha 数据集，供画像、策略和模型阶段复用。 */
    public static final String RAHA_DATASET = "rahaDataset";
    /** 步骤 1：数据加载阶段写入，表示输入数据快照元数据，供任务绑定快照和结果追溯使用。 */
    public static final String DATASET_SNAPSHOT = "datasetSnapshot";

    /** 步骤 2：策略计划阶段写入，表示当前快照生成的策略计划列表，供策略执行、特征和训练阶段使用。 */
    public static final String STRATEGY_PLANS = "strategyPlans";
    /** 步骤 2：策略计划阶段写入，表示策略计划的稳定版本，供训练和模型检测阶段记录产物血缘。 */
    public static final String STRATEGY_PLAN_VERSION = "strategyPlanVersion";
    /** 步骤 2：策略执行阶段写入，表示当前策略批量执行结果，供训练阶段构造样本和特征上下文。 */
    public static final String STRATEGY_BATCH_RESULT = "strategyBatchResult";
    /** 步骤 2：策略执行阶段写入，表示策略命中的候选异常单元格，供特征生成和规则检测阶段使用。 */
    public static final String STRATEGY_HITS = "strategyHits";

    /** 步骤 3：特征生成阶段写入，表示特征字典和稀疏向量结果，供聚类、采样、训练和检测阶段使用。 */
    public static final String FEATURE_ASSEMBLY_RESULT = "featureAssemblyResult";
    /** 步骤 3：聚类阶段写入，表示列内聚类批量结果，供采样、标签传播和训练阶段使用。 */
    public static final String CLUSTERING_BATCH_RESULT = "clusteringBatchResult";

    /** 步骤 4：采样阶段写入，表示生成或更新的待标注单元格任务，供真值标注阶段消费。 */
    public static final String ANNOTATION_TASKS = "annotationTasks";
    /** 步骤 4：采样阶段写入，表示采样评分、任务和指标汇总，供任务结果查看和后续分析使用。 */
    public static final String SAMPLING_BATCH_RESULT = "samplingBatchResult";
    /** 步骤 4：直接标注或真值标注阶段写入，表示当前任务可用的单元格标签，供标签传播和训练阶段使用。 */
    public static final String CELL_LABELS = "cellLabels";
    /** 步骤 4：标签传播阶段写入，表示由已知标签扩展出的传播结果，供模型训练阶段使用。 */
    public static final String LABEL_PROPAGATION_RESULT = "labelPropagationResult";

    /** 步骤 5：训练阶段写入，表示训练服务的统一执行结果，供结果持久化阶段判断保存位置。 */
    public static final String TRAIN_SERVICE_RESULT = "trainServiceResult";
    /** 步骤 5：训练阶段写入，表示训练业务输出，例如候选模型和训练摘要，供任务结果返回使用。 */
    public static final String TRAIN_OUTPUT = "trainOutput";
    /** 步骤 5：模型检测阶段写入，表示已发布模型检测服务的统一执行结果，供结果持久化阶段判断保存位置。 */
    public static final String DETECT_SERVICE_RESULT = "detectServiceResult";
    /** 步骤 5：模型检测阶段写入，表示已发布模型检测业务输出，供评估和任务结果返回使用。 */
    public static final String DETECT_OUTPUT = "detectOutput";
    /** 步骤 5：服务化采样阶段写入，表示采样服务的统一执行结果，供结果持久化阶段判断保存位置。 */
    public static final String SAMPLE_SERVICE_RESULT = "sampleServiceResult";
    /** 步骤 5：服务化采样阶段写入，表示采样业务输出，供任务结果返回和标注任务查看使用。 */
    public static final String SAMPLE_OUTPUT = "sampleOutput";

    /** 步骤 6：评估阶段写入，表示模型训练或检测后的评估结果，供任务结果返回和质量分析使用。 */
    public static final String EVALUATION_RESULT = "evaluationResult";
    /** 步骤 6：结果持久化阶段写入，表示最终结果逻辑位置，供任务完成后查询产物入口。 */
    public static final String RESULT_LOCATION = "resultLocation";
    /** 步骤 6：规则检测阶段写入，表示最终检测结果和指标，供评估、持久化和任务结果返回使用。 */
    public static final String DETECTION_BATCH_RESULT = "detectionBatchResult";

    private StageAttributeKeys() {
    }
}
