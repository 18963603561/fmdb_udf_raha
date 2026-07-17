# RahaUdfRequest 参数必填性与自动生成影响分析

## 一、分析范围

本文分析 `RahaUdfRequest` 的字段校验、表单解析、幂等提交、数据加载、快照生成和当前异步消费者调用链，目标是判断：

1. 当前哪些参数是真正的必填项；
2. 哪些参数可以改为选填项；
3. 哪些参数适合由代码、运行上下文或配置中心自动生成；
4. 自动生成后会对幂等、可追溯性、模型复现和数据一致性产生什么影响。

主要代码依据：

- `src/main/java/com/fiberhome/ml/raha/udf/RahaUdfRequest.java`
- `src/main/java/com/fiberhome/ml/raha/udf/RahaUdfRequestParser.java`
- `src/main/java/com/fiberhome/ml/raha/udf/RepositoryBackedRahaUdfJobSubmitter.java`
- `src/main/java/com/fiberhome/ml/raha/data/loader/SnapshotMetadataFactory.java`
- `src/main/java/com/fiberhome/ml/raha/data/loader/RowIdValidator.java`
- `src/main/java/com/fiberhome/ml/raha/fmdb/FmdbDatasetLoader.java`
- `src/main/java/com/fiberhome/ml/raha/app/RahaContainerValidationApplication.java`

## 二、结论摘要

`RahaUdfRequest` 中的字段并不都是人工必填项。

当前代码契约是：7 个公共必填参数、1 个公共选填参数，以及每种任务各 1 个专属必填参数。调用方选择不同 UDF 时，`taskType` 已由函数决定，不需要再填写。

从降低人工填写量的角度，建议分为四类：

| 分类 | 参数 | 建议 |
| --- | --- | --- |
| 已由系统确定 | `taskType` | 保持自动确定，不进入表单 |
| 可直接默认或从上下文取得 | `sourceType`、`caller`、`resultTable`、`labelingBudget` | 改为选填，系统生成有效值 |
| 可以自动生成，但必须补齐前置能力 | `snapshotId`、`idempotencyKey`、`modelVersion` | 有条件自动生成，必须冻结最终解析值 |
| 不应盲目自动生成 | `datasetId`、`rowIdColumn`、`annotationReference` | 优先通过数据集注册表或 FMDB 元数据取得，无法可靠取得时仍要求填写 |
| 核心输入 | `inputReference` | 当前形态下保留必填；引入数据集注册表后可由 `datasetId` 间接取得 |

推荐的最终方向不是在解析器中堆叠命名规则，而是建立“数据集注册表”。调用方原则上只填写 `datasetId` 和少量任务覆盖参数，系统从注册信息中取得输入表、主键、标注表和默认结果表。

## 三、当前代码中的真实必填规则

### 3.1 公共参数

`RahaUdfRequest` 构造函数在第 63 至 70 行执行公共参数校验。

| 参数 | 当前是否必填 | 当前校验 | 说明 |
| --- | --- | --- | --- |
| `taskType` | 是，但不是人工表单项 | 不能为空 | 由 `F_DW_RAHATRAIN`、`F_DW_RAHADETECT`、`F_DW_RAHASAMPLE` 固定传入 |
| `datasetId` | 是 | 非空白 | 逻辑数据集身份 |
| `inputReference` | 是 | 非空白；表来源时还校验表名 | FMDB 表名或只读 SQL |
| `sourceType` | 是 | 只允许 `FMDB_TABLE` 或 `FMDB_SQL` | 表单只允许 `TABLE` 或 `SQL` |
| `rowIdColumn` | 是 | 这里只校验非空白 | 加载阶段还会校验字段存在、非空且唯一 |
| `snapshotId` | 否 | 空白转换为 `null` | 加载阶段可以生成 |
| `idempotencyKey` | 是 | 非空白 | 重试去重和冲突检测依据 |
| `caller` | 是 | 非空白 | 当前仅用于日志和审计上下文 |
| `resultTable` | 是 | 非空白且符合 FMDB 表名格式 | 当前主要用于返回结果位置 |

### 3.2 任务专属参数

`validateTaskParameters()` 在第 80 行开始执行互斥校验。

| 任务 | 当前专属必填参数 | 禁止出现的参数 |
| --- | --- | --- |
| 训练 | `annotationReference` | `modelVersion`、非零 `labelingBudget` |
| 检测 | `modelVersion` | `annotationReference`、非零 `labelingBudget` |
| 采样 | 正整数 `labelingBudget` | `annotationReference`、`modelVersion` |

因此，当前每次提交实际需要人工编码 8 个参数：7 个公共必填参数，再加 1 个任务专属参数。`snapshotId` 可以不填，`taskType` 由所调用的 UDF 决定。

## 四、接口校验与实际执行之间的差异

当前仓库存在“请求已经严格要求参数，但执行链尚未完整消费参数”的现象。降低参数数量前，应先明确这是接口冗余，还是执行链未接通。

### 4.1 `toDataLoadRequest()` 当前没有进入主执行链

`RahaUdfRequest.toDataLoadRequest()` 会使用 `datasetId`、`inputReference`、`sourceType`、`rowIdColumn` 和 `snapshotId` 构造加载请求。但在当前仓库中，没有找到其他主代码调用该方法。

容器验收消费者 `dispatchUdfTask()` 使用的是预先加载的 `dirtyDataset`，只按 `taskType` 分支，没有从请求重新加载输入数据。这意味着当前验收链路并不能证明上述输入参数已真正控制任务执行。

### 4.2 三个任务专属参数目前主要停留在校验和序列化层

当前主代码中：

- `annotationReference` 没有被消费者读取；
- `modelVersion` 没有被消费者读取；
- `labelingBudget` 没有被消费者读取。

容器验收执行训练时使用内存中的采样和真值状态，检测时使用刚训练的上下文，采样预算则来自全局 `SamplingConfig`。因此，当前改变这三个请求值，可能只改变配置哈希和校验结果，不改变容器验收中的实际业务执行。

这属于需要优先确认的契约缺口。不能仅因为“当前没被使用”就直接删除参数；如果生产目标要求指定标注表、固定模型版本或覆盖采样预算，就应先把这些参数接入真实执行器。

### 4.3 `resultTable` 当前未决定实际结果写入表

`RepositoryBackedRahaUdfJobSubmitter.resultLocation()` 使用 `resultTable` 生成 `fmdb://<resultTable>/<jobId>` 回执地址，但提交器没有在这里写业务结果。

容器验收程序实际写结果时使用固定常量，而不是 `request.getResultTable()`。因此当前可能出现“回执声明的结果表”和“执行器实际写入表”不一致的风险。

## 五、逐项可选化与自动生成影响分析

### 5.1 `taskType`

建议：保持自动确定。

生成方式：由用户调用的 UDF 函数名称确定。

影响：无明显风险。继续禁止请求正文覆盖 `taskType`，可以避免函数语义和表单参数冲突。

### 5.2 `datasetId`

建议：当前保留必填；有数据集注册表后改为注册表主键，不建议直接从表名或 SQL 文本盲目生成。

可选生成方式：

1. 从 FMDB 数据目录中的数据集标识读取；
2. 表来源时将规范化表名映射为数据集标识；
3. SQL 来源时对规范化 SQL 生成哈希标识。

主要影响：

- `datasetId` 参与任务查询、模型归属、单元格标识和幂等范围；
- 表重命名或 SQL 格式变化可能生成新的数据集身份，导致历史模型、标注和画像无法复用；
- 直接使用哈希虽然稳定，但可读性和运维定位较差；
- 两张不同物理表可能属于同一逻辑数据集，简单按表名生成会割裂生命周期。

结论：只有在平台已有稳定的数据资产标识时才自动取得，否则继续人工填写。

### 5.3 `inputReference`

建议：当前保留必填；引入数据集注册表后由 `datasetId` 解析。

当前 UDF 只接收一个表单字符串，并不知道调用 SQL 所在的源表，无法从 Spark UDF 上下文可靠推断输入表。使用固定配置或注册表可以减少填写，但必须将解析后的实际表名或 SQL 固化进任务请求和配置哈希。

主要影响：若注册表中的映射被更新，而已提交任务只保存 `datasetId`，重试时可能读取不同数据源。任务提交时应保存解析后的 `inputReference` 和数据源版本。

### 5.4 `sourceType`

建议：改为选填，自动推断，仍允许显式覆盖。

推荐规则：

- 符合 FMDB 表名格式时识别为 `TABLE`；
- 去除前导空白后以 `SELECT` 或 `WITH` 开始时识别为 `SQL`；
- 不能唯一判断时拒绝请求，要求显式填写。

主要影响：风险较低，但带注释、括号或其他合法前缀的 SQL 可能被误判。推断逻辑必须和 `FmdbDatasetLoader.readSql()` 的只读 SQL 校验保持一致，不能出现解析器判定为 SQL、加载器又拒绝的情况。

### 5.5 `rowIdColumn`

建议：优先从 FMDB 主键元数据或数据集注册表取得；无法确定时保持必填。禁止默认使用行号、`monotonically_increasing_id` 或随机值。

可接受的自动方式：

1. 读取表的唯一主键元数据；
2. 使用数据集注册表中已审核的稳定行标识字段；
3. 只有明确配置的复合主键，才能生成稳定的组合行标识。

主要影响：

- 行号和分区相关标识在重读、重分区后可能变化；
- 全行哈希在任一字段被纠正后会变化，导致原单元格和标注失去关联；
- 全行重复数据会得到相同行标识，无法通过当前唯一性校验；
- 行标识变化会改变单元格坐标、特征、标注、检测结果和幂等写入主键。

结论：这是最不适合“为了省填写而随意生成”的字段。

### 5.6 `snapshotId`

建议：继续保持选填，但应改进自动生成依据后再作为默认生产方案。

当前行为：`SnapshotMetadataFactory` 在未提供 `snapshotId` 时，根据数据集、输入引用、数据源版本、模式哈希和行数生成。问题是 UDF 转换出的 `DataLoadRequest.sourceVersion` 为 `null`，此时工厂使用 `createdAt` 代替数据源版本。

主要影响：

- 相同数据在不同时间执行也会生成不同快照；
- `CellCoordinate` 将 `snapshotId` 纳入单元格哈希，因此快照变化会使全部单元格标识变化；
- 采样、训练和检测如果分别自动生成快照，可能无法共享标注、特征和检查点；
- 相同数据无法稳定复用历史产物，存储量和计算量增加；
- 仅使用模式哈希和行数也不能发现“行数不变但内容变化”。

推荐改法：优先使用 FMDB 表版本、事务提交号、分区批次、水位或平台快照号；这些都不可用时，再考虑对稳定主键和业务内容生成确定性内容指纹。最终解析出的快照必须写回持久化请求和任务回执。

### 5.7 `idempotencyKey`

建议：改为选填，但只在能够取得稳定快照或数据源版本后生成确定性幂等键。

推荐生成内容：任务类型、`datasetId`、解析后的 `inputReference`、稳定 `snapshotId`、最终有效配置版本和任务专属参数。

主要影响：

- 使用随机 UUID 自动生成会使每次重试都创建新任务，失去幂等价值；
- 只按请求配置生成而不包含有效快照，会把“配置相同但输入数据已更新”的任务错误判为重复；
- 完全确定性的键会让用户无法主动重跑同一快照和同一配置，需要保留显式覆盖键或增加 `runNonce`；
- 当前项目已有 `IdempotencyKeyGenerator`，但它面向 `RahaJobConfig`，UDF 请求应复用相同规范，避免两套算法产生不同语义。

结论：先解决稳定快照，再自动生成幂等键。顺序不能颠倒。

### 5.8 `caller`

建议：改为由认证上下文、Spark 会话用户或服务账号自动取得；无上下文时才允许显式传入受控值。

主要影响：

- 自动取得的身份比调用方自由填写更可信；
- 异步提交时必须把身份固化进请求，消费者不能再次读取自己的服务账号替代原调用者；
- 当前 `caller` 只用于日志和审计，不参与权限判断，因此从请求中移除不会改变当前授权结果；
- 如果平台无法提供用户身份，只记录统一服务账号会降低人工操作的审计粒度。

### 5.9 `resultTable`

建议：改为配置默认值或按任务类型配置，必要时允许管理员级覆盖。

推荐方式：训练、检测、采样分别配置受治理的默认结果表，或者统一写入固定表并用 `jobId`、`taskType` 分区。

主要影响：

- 减少人工填写和非法表名问题；
- 集中结果表有利于权限、模式和生命周期治理；
- 多租户环境必须加入租户隔离，不能让所有租户无条件共用同一表；
- 必须先让执行器真正使用解析后的有效结果表，否则只修改回执会继续产生位置不一致；
- 默认表发生配置变更时，提交阶段应把最终表名固化进任务，防止消费者读取新配置后写到另一张表。

### 5.10 `annotationReference`

建议：训练任务中有条件选填。优先从数据集注册表取得已审核的标注表；没有注册信息时保持必填。

不建议仅按 `<inputTable>_labels` 等命名规则自动拼接，因为错误关联标注表会直接污染训练数据和模型。

主要影响：

- 自动绑定正确标注表可以显著减少填写；
- 绑定错误比请求失败更危险，会产生看似成功但语义错误的模型；
- 应校验标注表存在、所属 `datasetId` 一致、快照兼容、行标识字段兼容和标签模式合法；
- 当前请求类只校验非空，没有像 `resultTable` 那样校验表名，也没有在现有消费者中使用该字段，接入自动绑定前需补齐执行链和校验。

### 5.11 `modelVersion`

建议：检测任务中改为选填，默认选择提交时刻“已发布且兼容”的模型，但必须保存解析后的实际版本。

主要影响：

- 省略后可以实现常规检测自动使用已发布模型；
- 如果消费者执行时才查询最新模型，提交和执行之间发生模型发布，会导致结果不可复现；
- 应在提交阶段解析并冻结模型版本，配置哈希、回执和日志都记录最终版本；
- 当前 `RahaDetectService` 按数据集和列加载已发布模型，UDF 请求中的单个 `modelVersion` 尚未接入这条加载链；
- Raha 是按列模型体系，一个请求只有一个 `modelVersion` 是否能完整表达整表检测，需要进一步明确。更合理的表达可能是“模型发布批次”或“每列模型版本映射”。

### 5.12 `labelingBudget`

建议：改为选填，默认使用 `raha.sampling.labeling-budget`，允许有权限的调用方覆盖。

主要影响：

- 项目已经存在全局 `SamplingConfig.labelingBudget`，具备直接默认的条件；
- 默认配置升级后，相同简化请求可能产生不同采样数量；
- 提交时必须把最终有效预算写入规范配置和持久化请求，不能由消费者在执行时重新读取可能已变化的配置；
- 当前容器验收执行本来就使用全局采样配置，没有读取请求中的 `labelingBudget`，应统一为单一有效值来源。

## 六、推荐的最小请求方案

### 6.1 不建设数据集注册表时

公共人工必填项建议保留为：

| 参数 | 原因 |
| --- | --- |
| `datasetId` | 无法从物理输入可靠推断逻辑数据集身份 |
| `inputReference` | 当前 UDF 上下文无法知道目标输入表或 SQL |
| `rowIdColumn` | 没有可靠主键元数据时不能安全生成 |

任务专属人工参数建议为：

| 任务 | 建议人工必填 | 建议选填覆盖 |
| --- | --- | --- |
| 训练 | `annotationReference` | 暂无 |
| 检测 | 无 | `modelVersion` |
| 采样 | 无 | `labelingBudget` |

系统负责生成或默认：`taskType`、`sourceType`、`caller`、`resultTable`、`snapshotId`、`idempotencyKey`，但 `snapshotId` 和 `idempotencyKey` 必须在完成稳定数据版本改造后一起落地。

这样，训练请求可从 8 个必填参数减少到 4 个，检测和采样可减少到 3 个。

### 6.2 建设数据集注册表后

数据集注册表至少保存：

| 注册信息 | 用途 |
| --- | --- |
| `datasetId` | 稳定逻辑身份 |
| `inputReference` | 默认输入表或受控 SQL |
| `sourceType` | 输入类型 |
| `rowIdColumn` 或复合主键 | 稳定行身份 |
| `annotationReference` | 默认标注表 |
| `resultTable` | 默认结果表 |
| 数据源版本解析策略 | 生成稳定快照 |
| 模型选择策略 | 选择已发布兼容模型 |

此时调用方可以只提供：

```text
datasetId=<数据集标识>
```

训练、检测和采样的差异由 UDF 名称决定，其他参数作为受控覆盖项存在。这个方案最能减少人工填写，同时比依赖表名后缀、默认列名等隐式约定更安全。

## 七、建议实施顺序

1. 先补齐真实执行链，确保数据加载、标注引用、模型选择、采样预算和结果表都使用请求的最终有效值。
2. 引入“原始请求”和“解析后有效请求”两个阶段，所有默认值在提交端一次性解析并冻结。
3. 为 `sourceType`、`caller`、`resultTable`、`labelingBudget` 增加默认解析，这些改造风险较低。
4. 建立 FMDB 数据集注册表或元数据解析接口，解决 `datasetId`、`rowIdColumn`、`annotationReference` 的可靠来源。
5. 使用 FMDB 数据版本生成稳定 `snapshotId`，禁止使用执行时间作为生产环境的主要版本来源。
6. 在稳定快照基础上生成确定性 `idempotencyKey`，同时保留显式覆盖或重跑标识。
7. 将检测默认策略改为提交时解析已发布兼容模型，并把实际模型版本或版本映射固化到请求和回执。
8. 增加参数省略、默认冻结、配置变更、重复提交、数据更新和跨任务快照一致性的集成测试。

## 八、最终建议

短期可以安全减少的人工参数是 `sourceType`、`caller`、`resultTable` 和 `labelingBudget`。其中 `caller` 从认证上下文取得，另外三个从输入解析或统一配置取得。

`snapshotId` 当前虽然已经选填，但现有自动生成包含执行时间，不适合直接作为生产稳定快照方案。`idempotencyKey` 必须等稳定快照能力完成后再自动生成，否则容易把变化后的数据误判为同一任务，或者让重试失去去重能力。

`datasetId`、`rowIdColumn` 和 `annotationReference` 不建议通过简单命名规则生成。建设数据集注册表后，才可以把它们从人工参数变为平台管理参数。

最后，当前最需要先处理的不是单纯放宽构造函数，而是让“表单参数、规范配置、持久化请求和实际执行器”共享同一套解析后的有效参数。否则参数看似减少了，回执、幂等和实际执行仍可能使用不同值。
