-- Raha 默认 FMDB 物理表，适用于 Spark SQL 3.3 和 ORC 表。
-- 所有语句必须保持幂等，服务初始化时会重复执行本脚本。
CREATE DATABASE IF NOT EXISTS dw;

-- 保存不可变采样行和待标注任务上下文。
CREATE TABLE IF NOT EXISTS dw.raha_sample_record (
    sample_batch_id STRING NOT NULL COMMENT '采样批次唯一标识，用于关联同一次采样产生的记录。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    input_reference STRING NOT NULL COMMENT '原始输入数据引用，例如文件路径、表名或任务输入位置。',
    source_version STRING COMMENT '输入数据源版本号，用于区分同一数据集的不同快照。',
    row_identity_mode STRING NOT NULL COMMENT '行身份生成模式，标识 row_id 的生成策略。',
    row_key_columns_json STRING COMMENT '行主键列配置，使用 JSON 保存参与行身份计算的字段列表。',
    row_fingerprint_algorithm STRING NOT NULL COMMENT '行指纹算法名称，用于说明 row_content_hash 的计算方式。',
    row_fingerprint_version STRING NOT NULL COMMENT '行指纹算法版本，用于兼容算法升级后的指纹差异。',
    row_id STRING NOT NULL COMMENT '样本行唯一标识，训练、标注和检测阶段通过该字段关联。',
    row_content_hash STRING NOT NULL COMMENT '行内容哈希值，用于识别样本行内容是否发生变化。',
    schema_hash STRING NOT NULL COMMENT '输入数据模式哈希值，用于判断字段结构是否一致。',
    column_schema_json STRING NOT NULL COMMENT '采样时的字段结构快照，使用 JSON 保存列名、类型和顺序。',
    row_data_json STRING NOT NULL COMMENT '采样行完整数据快照，使用 JSON 保存原始字段值。',
    duplicate_count BIGINT NOT NULL COMMENT '同一行指纹在采样源中出现的次数。',
    sampling_version STRING NOT NULL COMMENT '采样逻辑版本，用于追踪采样规则变化。',
    sampling_context_json STRING NOT NULL COMMENT '采样上下文参数，使用 JSON 保存配置、比例和过滤条件。',
    created_at BIGINT NOT NULL COMMENT '记录创建时间，毫秒级时间戳。',
    partition_month STRING NOT NULL COMMENT '月份分区字段，格式通常为 yyyyMM。'
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
COMMENT 'Raha 采样记录表，保存进入人工标注和后续训练流程的不可变样本行快照。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 追加保存标注批次元数据、原始行快照和用户标签。
CREATE TABLE IF NOT EXISTS dw.raha_annotation_record (
    annotation_batch_id STRING NOT NULL COMMENT '标注批次唯一标识。',
    sample_batch_id STRING NOT NULL COMMENT '关联的采样批次唯一标识。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    annotation_task_id STRING COMMENT '外部标注任务标识，没有外部任务时可为空。',
    row_id STRING NOT NULL COMMENT '被标注样本行唯一标识。',
    row_content_hash STRING NOT NULL COMMENT '被标注样本行内容哈希，用于校验标注时行内容是否匹配。',
    row_data_json STRING NOT NULL COMMENT '被标注样本行完整数据快照。',
    template_version STRING NOT NULL COMMENT '标注模板版本，用于解释 annotation_json 的结构。',
    file_name STRING COMMENT '标注文件名称或导入文件名称。',
    schema_hash STRING NOT NULL COMMENT '标注对应的数据模式哈希值。',
    annotation_json STRING NOT NULL COMMENT '用户标注结果，使用 JSON 保存单元格标签和补充信息。',
    annotator STRING COMMENT '标注人员、账号或系统来源。',
    batch_status STRING NOT NULL COMMENT '标注批次状态，例如草稿、有效、废弃或已替换。',
    batch_record_count BIGINT NOT NULL COMMENT '标注批次内记录总数。',
    valid_record_count BIGINT NOT NULL COMMENT '标注批次内有效记录数。',
    invalid_record_count BIGINT NOT NULL COMMENT '标注批次内无效记录数。',
    supersedes_batch_id STRING COMMENT '被当前批次替换的历史标注批次标识。',
    annotated_at BIGINT NOT NULL COMMENT '标注完成或导入时间，毫秒级时间戳。',
    partition_month STRING NOT NULL COMMENT '月份分区字段，格式通常为 yyyyMM。'
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
COMMENT 'Raha 标注记录表，追加保存人工标注批次、行快照和标注标签。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存采样快照检查点，供训练阶段复用 PROFILE、RUN_STRATEGY、FEATURE 和 CLUSTER 产物。
CREATE TABLE IF NOT EXISTS dw.raha_snapshot_checkpoint (
    checkpoint_id STRING NOT NULL COMMENT '检查点唯一标识。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    snapshot_id STRING NOT NULL COMMENT '采样或训练快照标识。',
    source_job_id STRING NOT NULL COMMENT '产生该检查点的任务标识。',
    sample_batch_id STRING COMMENT '关联的采样批次标识，非采样来源时可为空。',
    record_type STRING NOT NULL COMMENT '检查点记录类型，例如画像、策略、特征或聚类。',
    record_scope STRING NOT NULL COMMENT '检查点作用范围，例如数据集、列、行或单元格。',
    column_name STRING COMMENT '字段名称，列级或单元格级记录使用。',
    row_id STRING COMMENT '行唯一标识，行级或单元格级记录使用。',
    cell_id STRING COMMENT '单元格唯一标识，单元格级记录使用。',
    cell_value STRING COMMENT '单元格原始值或快照值。',
    artifact_version STRING COMMENT '产物版本，用于区分同一类型产物的不同生成逻辑。',
    profile_json STRING COMMENT '字段画像产物，使用 JSON 保存统计信息。',
    strategy_plan_json STRING COMMENT '策略计划产物，使用 JSON 保存候选检测策略。',
    strategy_hit_json STRING COMMENT '策略命中明细，使用 JSON 保存规则命中结果。',
    feature_dictionary_json STRING COMMENT '特征字典内容，使用 JSON 保存特征索引和定义。',
    feature_vector_json STRING COMMENT '特征向量内容，使用 JSON 保存单元格特征值。',
    feature_summary_json STRING COMMENT '特征摘要信息，使用 JSON 保存特征覆盖和分布。',
    cluster_version STRING COMMENT '聚类算法或聚类结果版本。',
    cluster_id STRING COMMENT '聚类簇标识。',
    cluster_distance DOUBLE COMMENT '样本到聚类中心或代表点的距离。',
    cluster_summary_json STRING COMMENT '聚类摘要信息，使用 JSON 保存簇统计结果。',
    payload_json STRING COMMENT '扩展载荷，保存当前 record_type 未显式建模的内容。',
    row_set_fingerprint STRING NOT NULL COMMENT '记录集合指纹，用于判断快照输入行集合是否一致。',
    config_fingerprint STRING NOT NULL COMMENT '任务配置指纹，用于判断检查点是否可复用。',
    schema_hash STRING NOT NULL COMMENT '数据模式哈希值。',
    source_version STRING COMMENT '输入数据源版本号。',
    created_at BIGINT NOT NULL COMMENT '检查点创建时间，毫秒级时间戳。',
    partition_month STRING NOT NULL COMMENT '月份分区字段，格式通常为 yyyyMM。'
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
COMMENT 'Raha 快照检查点表，保存画像、策略、特征和聚类等中间产物。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存列级训练产物，包括画像、策略、特征字典、聚类摘要和传播摘要。
CREATE TABLE IF NOT EXISTS dw.raha_training_column_artifact (
    training_batch_id STRING NOT NULL COMMENT '训练批次唯一标识。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识。',
    source_version STRING COMMENT '输入数据源版本号。',
    schema_hash STRING NOT NULL COMMENT '训练数据模式哈希值。',
    merge_algorithm_version STRING NOT NULL COMMENT '合并算法版本，用于追踪列级产物融合逻辑。',
    training_context_json STRING NOT NULL COMMENT '训练上下文参数，使用 JSON 保存配置、输入和运行信息。',
    column_name STRING NOT NULL COMMENT '训练字段名称。',
    profile_version STRING COMMENT '字段画像版本。',
    profile_json STRING COMMENT '字段画像内容，使用 JSON 保存统计结果。',
    strategy_plan_version STRING COMMENT '策略计划版本。',
    strategy_plan_json STRING COMMENT '策略计划内容，使用 JSON 保存候选策略。',
    feature_dictionary_version STRING COMMENT '特征字典版本。',
    feature_dictionary_json STRING COMMENT '特征字典内容，使用 JSON 保存特征定义。',
    cluster_version STRING COMMENT '聚类结果版本。',
    cluster_summary_json STRING COMMENT '聚类摘要内容，使用 JSON 保存簇统计信息。',
    propagation_summary_json STRING COMMENT '标签传播摘要，使用 JSON 保存传播策略和结果统计。',
    created_at BIGINT NOT NULL COMMENT '记录创建时间，毫秒级时间戳。'
)
USING ORC
COMMENT 'Raha 训练列级产物表，保存每个字段在训练阶段生成的画像、策略、特征和聚类摘要。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存全量训练单元格特征、聚类和标签传播结果。
CREATE TABLE IF NOT EXISTS dw.raha_training_cell (
    training_batch_id STRING NOT NULL COMMENT '训练批次唯一标识，也是表分区字段。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    training_snapshot_id STRING NOT NULL COMMENT '训练快照标识，用于关联同一次训练输入。',
    row_id STRING NOT NULL COMMENT '训练行唯一标识。',
    column_name STRING NOT NULL COMMENT '训练字段名称。',
    cell_id STRING NOT NULL COMMENT '训练单元格唯一标识。',
    cell_value STRING COMMENT '训练单元格原始值。',
    feature_dictionary_version STRING NOT NULL COMMENT '特征字典版本。',
    feature_vector_json STRING NOT NULL COMMENT '单元格特征向量，使用 JSON 保存稀疏或稠密特征。',
    feature_summary_json STRING NOT NULL COMMENT '单元格特征摘要，使用 JSON 保存可解释信息。',
    cluster_id STRING COMMENT '单元格所属聚类簇标识。',
    cluster_distance DOUBLE COMMENT '单元格到聚类中心或代表点的距离。',
    direct_label INT COMMENT '人工直接标注标签，通常 1 表示错误，0 表示正确。',
    propagated_label INT COMMENT '标签传播得到的标签，通常 1 表示错误，0 表示正确。',
    label_source STRING COMMENT '训练标签来源，例如人工标注、传播或规则生成。',
    source_annotation_batch_id STRING COMMENT '标签来源标注批次标识。',
    sample_weight DOUBLE COMMENT '训练样本权重。',
    created_at BIGINT NOT NULL COMMENT '记录创建时间，毫秒级时间戳。'
)
USING ORC
PARTITIONED BY (dataset_id, training_batch_id)
COMMENT 'Raha 训练单元格表，保存训练阶段全量单元格特征、聚类结果和标签信息。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存模型实际使用的单元格值、特征和最终标签。
CREATE TABLE IF NOT EXISTS dw.raha_training_example (
    model_set_version STRING NOT NULL COMMENT '模型集合版本，标识训练样本对应的模型发布集合。',
    training_batch_id STRING NOT NULL COMMENT '训练批次唯一标识。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    row_id STRING NOT NULL COMMENT '训练样本行唯一标识。',
    column_name STRING NOT NULL COMMENT '训练字段名称。',
    cell_id STRING NOT NULL COMMENT '训练单元格唯一标识。',
    cell_value STRING COMMENT '训练单元格原始值。',
    feature_dictionary_version STRING NOT NULL COMMENT '特征字典版本。',
    feature_vector_json STRING NOT NULL COMMENT '模型训练使用的特征向量，使用 JSON 保存。',
    label INT NOT NULL COMMENT '模型训练最终标签，通常 1 表示错误，0 表示正确。',
    label_source STRING NOT NULL COMMENT '最终标签来源，例如人工标注、传播或规则生成。',
    source_annotation_batch_id STRING COMMENT '标签来源标注批次标识。',
    sample_weight DOUBLE NOT NULL COMMENT '模型训练样本权重。',
    cluster_id STRING COMMENT '样本所属聚类簇标识。',
    created_at BIGINT NOT NULL COMMENT '记录创建时间，毫秒级时间戳。',
    partition_month STRING NOT NULL COMMENT '月份分区字段，格式通常为 yyyyMM。'
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
COMMENT 'Raha 训练样本表，保存模型训练实际消费的特征、标签和权重。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 保存模型集合和列模型载荷。
CREATE TABLE IF NOT EXISTS dw.raha_model_artifact (
    model_set_version STRING NOT NULL COMMENT '模型集合版本，用于标识一次可发布的模型集合。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识。',
    schema_hash STRING NOT NULL COMMENT '训练数据模式哈希值。',
    training_batch_id STRING NOT NULL COMMENT '模型来源训练批次唯一标识。',
    model_set_status STRING NOT NULL COMMENT '模型集合状态，例如训练中、已发布、已废弃。',
    state_version INT NOT NULL COMMENT '模型状态版本号，用于并发更新和幂等控制。',
    strategy_plan_version STRING NOT NULL COMMENT '模型对应的策略计划版本。',
    merge_algorithm_version STRING NOT NULL COMMENT '训练产物合并算法版本。',
    column_name STRING NOT NULL COMMENT '模型适用字段名称。',
    model_version STRING NOT NULL COMMENT '列模型版本。',
    classifier_type STRING NOT NULL COMMENT '分类器类型，例如逻辑回归、随机森林或外部模型类型。',
    feature_dictionary_version STRING NOT NULL COMMENT '模型使用的特征字典版本。',
    feature_dimension INT NOT NULL COMMENT '模型输入特征维度。',
    threshold DOUBLE NOT NULL COMMENT '模型判定阈值，分数达到或超过该值时判定为异常。',
    model_path STRING COMMENT '模型文件或模型目录存储路径。',
    model_payload_json STRING NOT NULL COMMENT '模型载荷内容，使用 JSON 保存内联模型参数或引用信息。',
    metrics_json STRING NOT NULL COMMENT '模型评估指标，使用 JSON 保存准确率、召回率等统计信息。',
    created_at BIGINT NOT NULL COMMENT '模型记录创建时间，毫秒级时间戳。',
    published_at BIGINT COMMENT '模型发布时间，毫秒级时间戳；未发布时为空。'
)
USING ORC
COMMENT 'Raha 模型产物表，保存模型集合状态和各字段列模型载荷。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 只保存错误单元格、具体原值和完整错误行。
CREATE TABLE IF NOT EXISTS dw.raha_detection_result (
    detection_batch_id STRING NOT NULL COMMENT '检测批次唯一标识。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    input_reference STRING NOT NULL COMMENT '本次检测输入数据引用，例如文件路径、表名或任务输入位置。',
    model_set_version STRING NOT NULL COMMENT '本次检测使用的模型集合版本。',
    model_version STRING NOT NULL COMMENT '本次检测使用的列模型版本。',
    row_id STRING NOT NULL COMMENT '被检测行唯一标识。',
    column_name STRING NOT NULL COMMENT '被检测字段名称。',
    cell_id STRING NOT NULL COMMENT '被检测单元格唯一标识。',
    original_value STRING COMMENT '被判定异常的单元格原始值。',
    row_data_json STRING NOT NULL COMMENT '异常所在行完整数据快照，使用 JSON 保存。',
    score DOUBLE NOT NULL COMMENT '模型输出异常分数。',
    threshold DOUBLE NOT NULL COMMENT '模型判定阈值，score 达到或超过该值时写入结果。',
    error_reason_json STRING NOT NULL COMMENT '异常原因说明，使用 JSON 保存模型、规则和特征解释。',
    detected_at BIGINT NOT NULL COMMENT '检测时间，毫秒级时间戳。',
    partition_date STRING NOT NULL COMMENT '日期分区字段，格式通常为 yyyyMMdd。'
)
USING ORC
PARTITIONED BY (dataset_id, partition_date)
COMMENT 'Raha 检测结果表，只保存判定为错误的单元格及其所在完整行快照。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 追加保存任务状态快照。
CREATE TABLE IF NOT EXISTS dw.raha_job_run (
    job_id STRING NOT NULL COMMENT '任务唯一标识。',
    state_version INT NOT NULL COMMENT '任务状态版本号，用于并发更新和幂等控制。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    idempotent_key STRING NOT NULL COMMENT '任务幂等键，用于避免重复提交同一业务任务。',
    job_type STRING NOT NULL COMMENT '任务类型，例如采样、训练或检测。',
    snapshot_id STRING COMMENT '任务关联快照标识。',
    config_version STRING NOT NULL COMMENT '任务配置版本。',
    status STRING NOT NULL COMMENT '任务状态，例如待运行、运行中、成功或失败。',
    current_stage_id STRING COMMENT '当前执行阶段标识。',
    result_summary_json STRING COMMENT '任务结果摘要，使用 JSON 保存输出数量、路径和统计信息。',
    error_code STRING COMMENT '任务失败错误码。',
    error_message STRING COMMENT '任务失败错误消息。',
    created_at BIGINT NOT NULL COMMENT '任务创建时间，毫秒级时间戳。',
    started_at BIGINT NOT NULL COMMENT '任务开始时间，毫秒级时间戳。',
    finished_at BIGINT NOT NULL COMMENT '任务结束时间，毫秒级时间戳；未结束时按业务约定填充。',
    updated_at BIGINT NOT NULL COMMENT '任务状态最后更新时间，毫秒级时间戳。',
    partition_month STRING NOT NULL COMMENT '月份分区字段，格式通常为 yyyyMM。'
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
COMMENT 'Raha 任务运行表，保存任务生命周期状态和结果摘要快照。'
TBLPROPERTIES ('raha.schema.version' = '1');

-- 合并保存阶段状态、重试尝试和检查点信息。
CREATE TABLE IF NOT EXISTS dw.raha_job_stage_attempt (
    job_id STRING NOT NULL COMMENT '所属任务唯一标识。',
    dataset_id STRING NOT NULL COMMENT '数据集唯一标识，也是表分区字段。',
    stage_id STRING NOT NULL COMMENT '任务阶段唯一标识。',
    stage_type STRING NOT NULL COMMENT '任务阶段类型，例如加载、画像、训练、检测或发布。',
    attempt_id INT NOT NULL COMMENT '阶段尝试序号，从业务约定的初始值递增。',
    state_version INT NOT NULL COMMENT '阶段状态版本号，用于并发更新和幂等控制。',
    checkpoint_id STRING COMMENT '阶段产生或复用的检查点标识。',
    status STRING NOT NULL COMMENT '阶段状态，例如待运行、运行中、成功、失败或跳过。',
    input_version_json STRING COMMENT '阶段输入版本信息，使用 JSON 保存上游产物版本。',
    input_fingerprint STRING COMMENT '阶段输入指纹，用于判断重试或复用时输入是否一致。',
    output_location STRING COMMENT '阶段输出位置，例如文件路径、表名或外部存储位置。',
    summary_json STRING COMMENT '阶段执行摘要，使用 JSON 保存记录数、耗时和核心指标。',
    error_code STRING COMMENT '阶段失败错误码。',
    error_message STRING COMMENT '阶段失败错误消息。',
    started_at BIGINT NOT NULL COMMENT '阶段尝试开始时间，毫秒级时间戳。',
    completed_at BIGINT NOT NULL COMMENT '阶段尝试完成时间，毫秒级时间戳；未完成时按业务约定填充。',
    updated_at BIGINT NOT NULL COMMENT '阶段状态最后更新时间，毫秒级时间戳。',
    partition_month STRING NOT NULL COMMENT '月份分区字段，格式通常为 yyyyMM。'
)
USING ORC
PARTITIONED BY (dataset_id, partition_month)
COMMENT 'Raha 任务阶段尝试表，保存每个阶段的执行状态、重试记录和检查点关联。'
TBLPROPERTIES ('raha.schema.version' = '1');
