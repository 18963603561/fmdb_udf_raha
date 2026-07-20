# fmdb_udf_raha

`fmdb_udf_raha` 提供 Raha 检测链路的 Hive/Spark `GenericUDF` 函数封装，用于在数仓 SQL 中完成样本采集、人工标注结果训练和模型执行检测。

当前提供三个函数：

| 功能 | 推荐函数名 | 实现类 | 含义 |
| --- | --- | --- | --- |
| 采集样本 | `F_DW_DETCOLLECT` | `com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT` | 采集检测样本并生成待标注压缩包 |
| 模型训练 | `F_DW_DETTRAIN` | `com.fiberhome.ml.raha.udf.F_DW_DETTRAIN` | 根据样本批次和人工标注训练检测模型 |
| 执行检测 | `F_DW_DETRUN` | `com.fiberhome.ml.raha.udf.F_DW_DETRUN` | 使用模型版本对表或 SQL 数据执行检测 |

## 构建

工程要求使用 JDK 8 构建。若本地不是 JDK 8，可在验证时临时跳过 enforcer。

```bash
mvn -q -DskipTests package
```

非 JDK 8 环境临时验证：

```bash
mvn -q -DskipTests "-Denforcer.skip=true" package
```

构建完成后，将生成的包含依赖包上传到 Hive 或 Spark SQL 可访问的位置。

## 函数注册

### 临时函数注册

```sql
ADD JAR /path/to/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETCOLLECT
AS 'com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT';

CREATE TEMPORARY FUNCTION F_DW_DETTRAIN
AS 'com.fiberhome.ml.raha.udf.F_DW_DETTRAIN';

CREATE TEMPORARY FUNCTION F_DW_DETRUN
AS 'com.fiberhome.ml.raha.udf.F_DW_DETRUN';
```

### 永久函数注册

如果平台支持永久函数，可按平台规范注册：

```sql
CREATE FUNCTION F_DW_DETCOLLECT
AS 'com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT'
USING JAR '/path/to/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar';

CREATE FUNCTION F_DW_DETTRAIN
AS 'com.fiberhome.ml.raha.udf.F_DW_DETTRAIN'
USING JAR '/path/to/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar';

CREATE FUNCTION F_DW_DETRUN
AS 'com.fiberhome.ml.raha.udf.F_DW_DETRUN'
USING JAR '/path/to/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar';
```

不同执行引擎对永久函数语法存在差异，以上语句可按实际 Hive、Spark SQL 或平台网关规范调整。

## 入参格式

三个函数均继承 `GenericUDF`，当前统一使用单字符串参数。字符串支持两种格式：

### 表单编码格式

```text
sourceType=TABLE&tableName=dw.customer&rowKeyColumns=id&publishZip=true
```

### JSON 格式

```json
{"sourceType":"TABLE","tableName":"dw.customer","rowKeyColumns":"id","publishZip":"true"}
```

字段值中的特殊字符建议使用 JSON 格式，或者对表单编码格式做 URL 编码。

## 公共入参

| 参数 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `sourceType` | 采集和检测建议传入 | 自动推断 | 数据来源类型，支持 `TABLE`、`SQL`。当传入 `tableName` 时推断为 `TABLE`，传入 `sqlText` 时推断为 `SQL` |
| `tableName` | `sourceType=TABLE` 时必填 | 无 | 待处理表名 |
| `sqlText` | `sourceType=SQL` 时必填 | 无 | 待处理查询语句，仅支持 `SELECT` 或 `WITH` 开头的查询 |
| `datasetId` | SQL 来源建议必填 | TABLE 来源按表名生成 | 数据集标识，用于关联快照、样本、模型和结果 |
| `snapshotId` | 否 | 自动生成 | 快照标识 |
| `sourceVersion` | 否 | 空 | 来源数据版本 |
| `rowKeyColumns` | 否 | 空 | 行主键字段，多个字段用英文逗号分隔 |
| `includeColumns` | 否 | 空 | 只处理指定字段，多个字段用英文逗号分隔 |
| `excludeColumns` | 否 | 空 | 排除指定字段，多个字段用英文逗号分隔 |
| `sensitiveColumns` | 否 | 空 | 敏感字段标记。当前检测明细按原值展示，不再输出脱敏展示值 |
| `publishZip` | 否 | `true` | 是否生成 ZIP 并输出 URL |
| `caller` | 否 | 空 | 调用方标识 |
| `requestId` | 否 | 自动生成 | 请求标识，便于日志排查 |

## 公共出参

三个函数均以二维表形式返回结果。公共字段如下：

| 字段 | 说明 |
| --- | --- |
| `status` | 执行状态，成功为 `SUCCESS`，失败为 `FAILED` |
| `errorCode` | 错误码，成功为空 |
| `errorMessage` | 错误说明，成功为空 |
| `datasetId` | 数据集标识 |
| `snapshotId` | 快照标识 |
| `sourceType` | 数据来源类型 |
| `inputReference` | 输入引用，表名或 SQL 摘要 |
| `createdAt` | 结果生成时间 |

## F_DW_DETCOLLECT

### 功能

采集函数用于读取表或 SQL 数据，生成检测样本批次、字段统计、聚类信息和待标注 Excel，并将待标注文件和辅助文件打包成 ZIP。

ZIP 文件中包含待标注 `xls` 文件和相关元数据文件。待标注文件名包含 `sampleBatchId`，标注上传后可根据文件名定位样本批次。

### 入参

| 参数 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| 公共入参 | 按公共规则 | 按公共规则 | 表或 SQL 来源配置 |
| `labelingBudget` | 否 | 系统默认 | 待标注样本预算 |
| `samplingRound` | 否 | `1` | 采样轮次 |
| `artifactBaseDir` | 否 | 系统默认 | 本次产物基础目录 |

### 出参

| 字段 | 说明 |
| --- | --- |
| 公共出参 | 公共返回字段 |
| `sourceVersion` | 来源数据版本 |
| `schemaHash` | 字段结构哈希 |
| `rowCount` | 采集数据量 |
| `fieldCount` | 字段数 |
| `validFieldCount` | 有效字段数 |
| `sampleBatchId` | 样本批次标识 |
| `sampleRecordCount` | 样本记录数 |
| `annotationTaskCount` | 待标注数量 |
| `clusterCount` | 聚类数量 |
| `clusteredFieldCount` | 已聚类字段数 |
| `annotationExcelName` | 待标注 Excel 文件名 |
| `annotationZipName` | 待标注 ZIP 文件名 |
| `annotationZipUrl` | 待标注 ZIP 下载地址 |
| `partitionMonth` | 分区月份 |

### 调用示例

```sql
SELECT *
FROM F_DW_DETCOLLECT(
  'sourceType=TABLE&tableName=dw.customer&rowKeyColumns=id&publishZip=true'
);
```

```sql
SELECT *
FROM F_DW_DETCOLLECT(
  '{"sourceType":"SQL","datasetId":"customer_query","sqlText":"select * from dw.customer where dt=''20260720''","rowKeyColumns":"id"}'
);
```

### 文件命名

待标注 Excel 文件名采用可定位样本批次的命名方式：

```text
raha_annotation_<datasetId>_<sampleBatchId>.xls
```

待标注 ZIP 文件名采用：

```text
raha_annotation_<datasetId>_<sampleBatchId>.zip
```

## F_DW_DETTRAIN

### 功能

训练函数根据 `sampleBatchId` 定位样本批次和人工标注文件，导入标注结果并训练模型，返回模型版本、训练状态和质量指标。

训练时优先从默认 HDFS 标注上传目录查找最新文件：

```text
/fmdb/detection/annotation/
```

如果默认目录找不到最新文件，则从已导入的标注结果记录中获取。仍获取不到时返回 `MANUAL_ANNOTATION_NOT_FOUND`，错误信息会提示将人工标注文件上传到 `/fmdb/detection/annotation/`。

模型默认写入：

```text
/fmdb/detection/model/
```

### 入参

| 参数 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `sampleBatchId` | 是 | 无 | 样本批次标识 |
| `annotationDir` | 否 | `/fmdb/detection/annotation/` | 人工标注文件上传目录 |
| `allowPartialAnnotation` | 否 | `false` | 是否允许部分标注训练 |
| `modelNamePrefix` | 否 | `raha` | 模型名称前缀 |
| `modelBasePath` | 否 | `/fmdb/detection/model/` | 模型存储基础路径 |
| `publishZip` | 否 | `true` | 是否生成训练报告 ZIP |
| 公共入参 | 否 | 按公共规则 | 可用于覆盖训练输入来源 |

### 出参

| 字段 | 说明 |
| --- | --- |
| 公共出参 | 公共返回字段 |
| `sampleBatchId` | 样本批次标识 |
| `annotationBatchId` | 标注批次标识 |
| `annotationFileName` | 使用的标注文件名 |
| `annotationStatus` | 标注状态 |
| `annotationRecordCount` | 标注记录数 |
| `validAnnotationCount` | 有效标注数 |
| `invalidAnnotationCount` | 无效标注数 |
| `modelSetVersion` | 模型集合版本 |
| `columnName` | 字段名 |
| `modelVersion` | 字段模型版本 |
| `modelStatus` | 模型状态 |
| `classifierType` | 分类器类型 |
| `featureDictionaryVersion` | 特征字典版本 |
| `strategyPlanVersion` | 策略计划版本 |
| `threshold` | 检测阈值 |
| `metricJson` | 训练指标 JSON |
| `reportZipName` | 训练报告 ZIP 文件名 |
| `reportZipUrl` | 训练报告 ZIP 下载地址 |

### 调用示例

```sql
SELECT *
FROM F_DW_DETTRAIN(
  'sampleBatchId=sample_20260720143000_customer&allowPartialAnnotation=false'
);
```

```sql
SELECT *
FROM F_DW_DETTRAIN(
  '{"sampleBatchId":"sample_20260720143000_customer","annotationDir":"/fmdb/detection/annotation/","modelBasePath":"/fmdb/detection/model/"}'
);
```

## F_DW_DETRUN

### 功能

检测函数使用指定 `modelSetVersion` 对表或 SQL 数据执行检测，输出检测数据基本信息、预测错误量和明细 ZIP 地址。

检测明细以 Excel 输出。字段值按原值展示，当前不再输出 `masked_value` 脱敏展示值。

### 入参

| 参数 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| 公共入参 | 按公共规则 | 按公共规则 | 表或 SQL 来源配置 |
| `modelSetVersion` | 是 | 无 | 模型集合版本 |
| `missingModelPolicy` | 否 | `PARTIAL` | 字段模型缺失策略，支持 `FAIL`、`PARTIAL` |
| `detailFormat` | 否 | `xls` | 明细文件格式，当前固定为 `xls` |
| `publishZip` | 否 | `true` | 是否生成检测明细 ZIP |

### 出参

| 字段 | 说明 |
| --- | --- |
| 公共出参 | 公共返回字段 |
| `modelSetVersion` | 使用的模型集合版本 |
| `schemaHash` | 字段结构哈希 |
| `rowCount` | 检测数据量 |
| `fieldCount` | 字段数 |
| `validFieldCount` | 有效字段数 |
| `modelFieldCount` | 命中模型的字段数 |
| `failedFieldCount` | 检测失败字段数 |
| `detectedCellCount` | 参与检测单元格数 |
| `detectedErrorCount` | 预测错误量 |
| `resultTable` | 检测结果表或结果引用 |
| `detailZipName` | 检测明细 ZIP 文件名 |
| `detailZipUrl` | 检测明细 ZIP 下载地址 |

### 检测明细字段

| 字段 | 说明 |
| --- | --- |
| `dataset_id` | 数据集标识 |
| `snapshot_id` | 快照标识 |
| `row_id` | 行标识 |
| `column_name` | 字段名 |
| `original_value` | 原始字段值 |
| `score` | 异常分数 |
| `threshold` | 阈值 |
| `detected_as_error` | 是否预测为错误 |
| `model_name` | 模型名称 |
| `model_version` | 模型版本 |
| `feature_dictionary_version` | 特征字典版本 |
| `strategy_ids` | 命中策略列表 |
| `reasons_json` | 预测原因 JSON |

### 调用示例

```sql
SELECT *
FROM F_DW_DETRUN(
  'sourceType=TABLE&tableName=dw.customer&rowKeyColumns=id&modelSetVersion=model-set-20260720150000&missingModelPolicy=PARTIAL'
);
```

```sql
SELECT *
FROM F_DW_DETRUN(
  '{"sourceType":"SQL","datasetId":"customer_query","sqlText":"select * from dw.customer where dt=''20260720''","modelSetVersion":"model-set-20260720150000"}'
);
```

## 默认配置

默认配置位于 `src/main/resources/raha-defaults.properties`。

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `raha.output.work-dir` | `/tmp/raha-udf-output` | 本地临时产物目录 |
| `raha.output.local-web-root` | `/tmp/raha-udf-web` | 本地 Web 发布目录 |
| `raha.output.hdfs-export-path` | `/fmdb/detection/output/` | HDFS 产物导出目录 |
| `raha.output.web-base-url` | 空 | ZIP 下载地址前缀，为空时按运行环境生成 |
| `raha.annotation.upload-dir` | `/fmdb/detection/annotation/` | 人工标注上传默认 HDFS 路径 |
| `raha.model.base-path` | `/fmdb/detection/model/` | 模型默认存储路径 |
| `raha.runtime.storage-mode` | `FMDB` | 运行时存储模式 |

## 返回状态与错误码

| 错误码 | 说明 |
| --- | --- |
| `INVALID_ARGUMENT` | 入参错误或必填参数缺失 |
| `DATA_LOAD_FAILED` | 数据加载失败 |
| `SAMPLE_NOT_FOUND` | 样本批次不存在 |
| `SAMPLE_OUTPUT_NOT_FOUND` | 样本采集产物不存在 |
| `ANNOTATION_FILE_NOT_FOUND` | 标注文件不存在 |
| `MANUAL_ANNOTATION_NOT_FOUND` | 未找到人工标注数据 |
| `ANNOTATION_FILE_INVALID` | 标注文件格式错误或内容无效 |
| `ANNOTATION_BATCH_MISMATCH` | 标注文件与样本批次不匹配 |
| `ANNOTATION_IMPORT_FAILED` | 标注导入失败 |
| `TRAINING_FAILED` | 模型训练失败 |
| `TRAIN_OUTPUT_NOT_FOUND` | 训练产物不存在 |
| `MODEL_SET_NOT_FOUND` | 模型集合版本不存在 |
| `MODEL_SET_NOT_PUBLISHED` | 模型集合未发布或不可用 |
| `DETECTION_RUN_FAILED` | 检测执行失败 |
| `DETECT_OUTPUT_NOT_FOUND` | 检测产物不存在 |
| `PUBLISH_FAILED` | ZIP 或 URL 发布失败 |
| `UNEXPECTED_ERROR` | 未预期异常 |

## 相关实现

| 类型 | 路径 |
| --- | --- |
| UDF 函数 | `src/main/java/com/fiberhome/ml/raha/udf/` |
| UDF 服务 | `src/main/java/com/fiberhome/ml/raha/udf/service/` |
| 标注文件定位 | `src/main/java/com/fiberhome/ml/raha/annotation/service/` |
| ZIP 和 Excel 发布 | `src/main/java/com/fiberhome/ml/raha/output/publish/` |
| 默认配置 | `src/main/resources/raha-defaults.properties` |
| 契约与方案文档 | `doc/20260720/Raha三函数UDF契约与实现方案-202607201450.md` |
