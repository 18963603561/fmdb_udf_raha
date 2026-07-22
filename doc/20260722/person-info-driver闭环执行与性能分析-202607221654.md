# person_info Driver 闭环执行与性能分析

## 一、执行结论

本次已使用 `src/main/java/com/fiberhome/ml/raha/app/RahaUdfDriverApp.java` 完成 `person_info` 数据集的模型采集、自动标注、训练和预测闭环。

整体结果如下：

| 阶段 | 结果 | 关键输出 |
|---|---|---|
| 模型采集 | 成功 | `sample_dw.person_info@20260722084354.495` |
| 自动标注 | 成功 | 300 条全部匹配，异常 83 条，正常 217 条 |
| 模型训练 | 成功 | `dw.person_info@20260722085029.808-job-cc210f21-1c41-4372-9715-2ba865c09027` |
| 模型预测 | 成功 | 450 行，2700 个单元格，检测异常单元格 215 个 |

## 二、请求文件变更

| 文件 | 变更说明 |
|---|---|
| `datasets/person_info/requests/collect-local.json` | 使用 SQL 采集 `select * from dw.person_info limit 451`，开启强制运行，`forceRunId=c1` |
| `datasets/person_info/requests/train-local.json` | 绑定本次采样批次和快照，使用 `annotation-upload` 目录内自动标注文件训练 |
| `datasets/person_info/requests/detect-local-limit450.json` | 绑定本次训练输出的 `modelSetVersion` 执行预测，`forceRunId=d1` |

训练请求当前关键字段：

```json
{
  "sampleBatchId": "sample_dw.person_info@20260722084354.495",
  "snapshotId": "snapshot_dw.person_info@person-info-run-20260722164127",
  "annotationDir": "F:\\ai-code\\fmdb_udf_raha\\datasets\\person_info\\annotation-upload",
  "modelNamePrefix": "person_info_r7",
  "forceRun": true,
  "forceRunId": "t1"
}
```

预测请求当前关键字段：

```json
{
  "modelSetVersion": "dw.person_info@20260722085029.808-job-cc210f21-1c41-4372-9715-2ba865c09027",
  "forceRun": true,
  "forceRunId": "d1"
}
```

## 三、标注产物

采集标准包：

`datasets/person_info/web/2026/7/22/raha-collect_sample_dw.person_info_20260722084354.495_20260722164403.zip`

提取出的标准标注文件：

`datasets/person_info/tmp/collect-20260722084354/annotation/raha-annotation_sample_dw.person_info_20260722084354.495_20260722164403.xls`

自动标注后上传目录文件：

`datasets/person_info/annotation-upload/raha-annotation_sample_dw.person_info_20260722084354.495_20260722164403_labeled.xls`

自动标注统计：

| 指标 | 数值 |
|---|---:|
| 标注行数 | 300 |
| 真值匹配行数 | 300 |
| 未匹配行数 | 0 |
| 异常行数 | 83 |
| 正常行数 | 217 |

## 四、采集阶段性能

采集任务：

`job-53d31902-3235-48c3-b820-227429934c3c`

采集阶段总耗时：

`52695 ms`，约 `52.70 s`

| 阶段 | 耗时 ms | 约合秒 | 说明 |
|---|---:|---:|---|
| LOAD_DATA | 4662 | 4.66 | 加载源数据 |
| PROFILE | 9563 | 9.56 | 数据画像 |
| GENERATE_STRATEGY | 7 | 0.01 | 生成策略 |
| RUN_STRATEGY | 29839 | 29.84 | 执行策略 |
| GENERATE_FEATURE | 1328 | 1.33 | 生成特征 |
| CLUSTER | 523 | 0.52 | 聚类 |
| SNAPSHOT_CHECKPOINT | 2749 | 2.75 | 保存快照检查点 |
| SAMPLE | 3526 | 3.53 | 样本选择 |
| PERSIST_RESULT | 498 | 0.50 | 持久化采集结果 |

采集阶段主要耗时集中在 `RUN_STRATEGY`，约占阶段总耗时的 `56.63%`。

## 五、训练阶段性能

训练任务：

`job-cc210f21-1c41-4372-9715-2ba865c09027`

训练命令墙钟耗时：

`113.8 s`

训练阶段总耗时：

`35515 ms`，约 `35.52 s`

| 阶段 | 耗时 ms | 约合秒 | 说明 |
|---|---:|---:|---|
| RESTORE_SNAPSHOT_CHECKPOINT | 1114 | 1.11 | 恢复采样快照中间结果 |
| LABEL | 0 | 0.00 | 导入标注 |
| PROPAGATE | 56 | 0.06 | 标签传播 |
| TRAIN | 33393 | 33.39 | 训练字段模型 |
| PERSIST_RESULT | 952 | 0.95 | 持久化模型和训练报告 |

训练阶段主要耗时集中在 `TRAIN`，约占阶段总耗时的 `94.02%`。

字段模型训练结果：

| 字段 | 状态 | 训练 F1 | 训练精确率 | 训练召回率 |
|---|---|---:|---:|---:|
| age | PUBLISHED | 1.0000 | 1.0000 | 1.0000 |
| email | PUBLISHED | 1.0000 | 1.0000 | 1.0000 |
| home_address | PUBLISHED | 1.0000 | 1.0000 | 1.0000 |
| id_card | PUBLISHED | 0.9268 | 1.0000 | 0.8636 |
| name | PUBLISHED | 1.0000 | 1.0000 | 1.0000 |
| phone | PUBLISHED | 1.0000 | 1.0000 | 1.0000 |

训练报告包：

`http://127.0.0.1:18080/2026/7/22/raha-dettrain_dw.person_info_20260722165109.zip`

## 六、预测阶段性能

预测任务：

`job-1db4324f-5865-4079-8a80-55343acffea7`

预测命令墙钟耗时：

`106.4 s`

预测阶段总耗时：

`42453 ms`，约 `42.45 s`

| 阶段 | 耗时 ms | 约合秒 | 说明 |
|---|---:|---:|---|
| LOAD_DATA | 4442 | 4.44 | 加载预测源数据 |
| PROFILE | 10350 | 10.35 | 数据画像 |
| GENERATE_STRATEGY | 5 | 0.01 | 生成策略 |
| RUN_STRATEGY | 22893 | 22.89 | 执行策略 |
| GENERATE_FEATURE | 805 | 0.81 | 生成特征 |
| PREDICT | 3657 | 3.66 | 模型预测 |
| PERSIST_RESULT | 301 | 0.30 | 持久化预测结果 |

预测阶段主要耗时集中在 `RUN_STRATEGY`，约占阶段总耗时的 `53.93%`。

预测输出：

| 指标 | 数值 |
|---|---:|
| 行数 | 450 |
| 字段数 | 6 |
| 有效字段数 | 6 |
| 模型字段数 | 6 |
| 失败字段数 | 0 |
| 检测单元格数 | 2700 |
| 异常单元格数 | 215 |

预测明细包：

`http://127.0.0.1:18080/2026/7/22/raha-detrun_dw.person_info_20260722165333.zip`

## 七、运行告警分析

本次训练和预测均成功，日志中存在以下可关注告警：

| 告警 | 影响判断 | 建议 |
|---|---|---|
| 远端 Web 发布失败并降级本地 Web | 非致命，结果已写入本地 Web 目录，URL 使用 `127.0.0.1` 可访问 | 如果需要远端访问，检查 SSH `192.168.56.1:22034` 和目标目录权限 |
| 训练中 `home_address`、`phone` 出现传播训练集不可用或类别极端失衡回退直接标签 | 非致命，模型仍发布成功 | 后续可增加对应字段异常样本，提升传播训练稳定性 |
| 预测中模型策略计划版本与运行时策略计划版本不同 | 非致命，系统完成了特征维度校验后继续预测 | 若要完全消除告警，需要让训练和预测使用完全一致的策略计划生成输入 |
| 预测中部分字段字典版本不同 | 非致命，系统完成结构校验和版本映射后继续预测 | 如果后续出现字段缺失或分数异常，再重点排查字典版本差异 |

## 八、结论

本次闭环已经验证：标准采集文件可以提取并自动标注，训练请求可以通过 `sampleBatchId` 和 `snapshotId` 复用采样中间结果完成训练，预测请求可以直接使用训练输出的 `modelSetVersion` 完成检测。

当前最需要优化的性能点不是模型预测本身，而是采集和预测阶段的 `RUN_STRATEGY`，以及训练阶段的 `TRAIN`。从本次规模看，阶段内核心耗时分别约为 `29.84 s`、`22.89 s` 和 `33.39 s`。
