# StrategyPlanGenerator 功能分析

## 一、类的定位

`StrategyPlanGenerator` 位于 `src/main/java/com/fiberhome/ml/raha/strategy/plan/StrategyPlanGenerator.java`，核心职责是根据已经完成画像的 `RahaDataset` 和用户传入的 `StrategyConfig`，生成一批状态为 `PLANNED` 的 `StrategyPlan`。

它本身不执行检测，也不访问数据内容；它只基于字段元数据、字段画像和策略配置，决定“后续应该跑哪些策略、作用在哪些字段、携带哪些参数、优先级是多少”。

可以把它理解成策略执行前的编排器：

1. 输入：数据集结构、字段画像、策略开关、字段白名单黑名单、策略类型白名单黑名单、策略数量上限。
2. 处理：按字段生成 OD、PVD 策略；按字段对生成 RVD 策略。
3. 输出：排序后的不可修改 `List<StrategyPlan>`。

## 二、主要入口

入口方法是 `generate(RahaDataset dataset, StrategyConfig config)`。

主要流程如下：

1. 校验 `dataset` 和 `config` 不能为空。
2. 遍历数据集的每个字段。
3. 通过 `isColumnEnabled` 判断字段是否参与策略生成。
4. 从 `dataset.getProfiles()` 里取字段画像。
5. 如果字段没有画像或非空值数量为 0，只尝试生成空值占位符策略。
6. 如果启用了 `OD`，为字段生成异常值检测策略。
7. 如果启用了 `PVD`，为字段生成模式违规检测策略。
8. 如果启用了 `RVD`，为字段对生成一对多关系检测策略。
9. 按优先级升序排序；优先级相同时按 `strategyId` 升序排序。
10. 如果超过 `maxStrategyCount`，按排序结果截断。
11. 返回不可修改列表。

## 三、字段过滤规则

字段是否参与生成由 `isColumnEnabled` 控制：

1. 字段自身必须是 `detectable=true`。
2. 如果 `includedColumns` 为空，则不做白名单限制。
3. 如果 `includedColumns` 非空，则字段名必须在白名单里。
4. 字段名不能出现在 `excludedColumns` 里。

示例：

```text
字段: name, age, payload
includedColumns = [name, age]
excludedColumns = [age]

最终参与字段:
name 参与
age 被 excludedColumns 排除
payload 不在 includedColumns 中，也被排除
```

## 四、策略类型过滤规则

每个具体策略在加入计划前，都会经过 `isStrategyTypeEnabled` 判断：

1. 如果 `includedStrategyTypes` 为空，则不做策略类型白名单限制。
2. 如果 `includedStrategyTypes` 非空，则策略类型必须在白名单里。
3. 策略类型不能出现在 `excludedStrategyTypes` 里。

示例：

```text
includedStrategyTypes = [OD_LOW_FREQUENCY, PVD_NULL_PLACEHOLDER]
excludedStrategyTypes = []

最终只会生成这两种类型:
OD_LOW_FREQUENCY
PVD_NULL_PLACEHOLDER
```

## 五、OD 策略生成规则

OD 表示异常值检测策略，生成逻辑在 `addOdPlans`。

### 1. OD_LOW_FREQUENCY

只要字段参与检测且有非空画像，就会尝试生成低频值策略。

核心参数：

```text
strategyType = OD_LOW_FREQUENCY
maxFrequency = max(1, floor(nonNullCount * lowFrequencyRatio))
```

例子：

```text
字段 age:
nonNullCount = 1000
lowFrequencyRatio = 0.01

maxFrequency = max(1, floor(1000 * 0.01)) = 10
```

含义是：后续执行该策略时，出现次数小于或等于 10 的值可能被视为低频异常候选。

### 2. OD_NUMERIC_DISTANCE

只有数值画像充分时才生成。

触发条件：

```text
numericCount >= minimumNumericCount
numericMean != null
numericStandardDeviation != null
numericStandardDeviation > 0
```

生成参数：

```text
strategyType = OD_NUMERIC_DISTANCE
numericMean = 字段均值
numericStandardDeviation = 字段标准差
zThreshold = 配置中的 Z 分数阈值
```

例子：

```text
字段 salary:
numericCount = 500
numericMean = 10000
numericStandardDeviation = 2000
zThreshold = 3

后续执行时，距离均值超过 3 个标准差的值可能成为异常候选。
例如 salary = 18000:
z = abs(18000 - 10000) / 2000 = 4
超过阈值 3，因此可能被识别为异常。
```

### 3. OD_QUANTILE

只有四分位画像充分时才生成。

触发条件：

```text
numericCount >= minimumQuantileCount
numericQ1 != null
numericQ3 != null
numericQ3 > numericQ1
```

生成参数：

```text
strategyType = OD_QUANTILE
numericQ1 = 第一四分位数
numericQ3 = 第三四分位数
iqrMultiplier = 四分位距倍数
```

例子：

```text
字段 price:
numericQ1 = 20
numericQ3 = 100
iqrMultiplier = 1.5

IQR = 100 - 20 = 80
下界 = 20 - 1.5 * 80 = -100
上界 = 100 + 1.5 * 80 = 220

后续执行时，price > 220 或 price < -100 的值可能成为异常候选。
```

## 六、PVD 策略生成规则

PVD 表示模式违规检测策略，生成逻辑在 `addPvdPlans`。

### 1. PVD_CHARACTER_SET

只要字段参与检测且有非空画像，就会尝试生成字符集策略。

生成参数：

```text
strategyType = PVD_CHARACTER_SET
minorityRatio = 少数模式比例阈值
```

例子：

```text
字段 phone:
大部分值只包含数字和短横线，例如 138-0000-0000
少量值包含中文或特殊符号

该策略后续会倾向于把少数异常字符集模式标为候选问题。
```

### 2. PVD_LENGTH

只有字段长度有变化时才生成。

触发条件：

```text
minLength >= 0
maxLength > minLength
```

生成参数：

```text
strategyType = PVD_LENGTH
minorityRatio = 少数模式比例阈值
iqrMultiplier = 四分位距倍数
```

例子：

```text
字段 id_card:
绝大多数长度为 18
少量长度为 10 或 30

如果画像中 minLength 和 maxLength 不相等，则生成长度异常策略。
```

### 3. PVD_NULL_PLACEHOLDER

该策略用于识别空值或特殊占位值。

触发条件：

```text
启用了 PVD
profile != null
totalCount > 0
```

生成参数：

```text
strategyType = PVD_NULL_PLACEHOLDER
placeholders = 配置中的占位符字符串
```

例子：

```text
placeholders = "NULL,N/A,-,UNKNOWN"

字段 email 中出现 "N/A" 或 "UNKNOWN" 时，后续策略执行可能将这些值识别为空值占位问题。
```

注意：如果字段画像存在且总行数大于 0，即使该字段 `nonNullCount = 0`，入口逻辑也会尝试为它生成该策略。

### 4. PVD_TYPE_FORMAT

只要字段参与检测且有非空画像，就会尝试生成类型格式策略。

生成参数：

```text
strategyType = PVD_TYPE_FORMAT
minorityRatio = 少数模式比例阈值
formatType = 格式类型
formatMinRatio = 格式适用最小比例
```

例子：

```text
字段 birthday:
大部分值为 2026-07-18
少量值为 07/18/2026 或 abc
formatType = AUTO
formatMinRatio = 0.8

后续执行时，可以自动识别主流日期格式，并把偏离主流格式的值作为候选问题。
```

## 七、RVD 策略生成规则

RVD 表示关系违规检测策略，生成逻辑在 `addRvdPlans`。

当前只生成一种策略：`RVD_ONE_TO_MANY`。

生成前提：

1. 配置启用了 `RVD` 策略族。
2. 策略类型 `RVD_ONE_TO_MANY` 未被策略类型过滤排除。
3. 字段通过 `isColumnEnabled`。
4. 字段类型不是数组、映射、结构体、二进制等复杂类型。
5. 字段画像存在且 `nonNullCount > 0`。

字段类型过滤逻辑：

```text
dataType 小写后不能包含:
array
map
struct
binary
```

生成方式是有方向的两两组合。

例子：

```text
可用字段:
city
province
zipcode

可能生成 6 个字段对:
city -> province
city -> zipcode
province -> city
province -> zipcode
zipcode -> city
zipcode -> province
```

每个计划的配置类似：

```text
strategyType = RVD_ONE_TO_MANY
leftColumn = city
rightColumn = province
```

含义是后续执行时检查左字段到右字段是否存在一对多冲突。例如同一个 `zipcode` 是否对应多个 `city`。

`maxRvdColumnPairs` 会限制 RVD 字段对数量。如果可生成字段对太多，达到上限后会停止继续生成，并记录告警日志。

## 八、优先级、策略标识和不可变输出

每个策略计划会生成以下核心信息：

```text
strategyId
strategyFamily
targetColumns
configuration
priority
status = PLANNED
```

`strategyId` 由 `StrategyIdentityGenerator.strategyId` 生成，输入包括策略族、目标字段和配置。这样相同字段和相同参数会得到稳定标识，便于后续重放、去重或版本管理。

优先级规则：

1. 如果 `config.getStrategyPriorities()` 中配置了当前 `strategyType` 的优先级，则使用配置值。
2. 否则使用 `StrategyGenerationConfig` 中的默认优先级。
3. 数值越小越优先。
4. 最终按 `priority` 升序排序。
5. 优先级相同则按 `strategyId` 升序排序，保证结果稳定。

输出列表使用 `Collections.unmodifiableList` 包装，调用方不能直接修改返回结果。

## 九、完整示例

假设有数据集 `customer_snapshot_001`，字段如下：

```text
customer_id: string, detectable=true
age: int, detectable=true
email: string, detectable=true
raw_payload: binary, detectable=true
remark: string, detectable=false
```

字段画像：

```text
customer_id:
totalCount=1000, nonNullCount=1000, minLength=8, maxLength=8

age:
totalCount=1000, nonNullCount=980, numericCount=980
numericMean=35, numericStandardDeviation=10
numericQ1=28, numericQ3=42
minLength=1, maxLength=3

email:
totalCount=1000, nonNullCount=900
minLength=5, maxLength=40

raw_payload:
totalCount=1000, nonNullCount=1000

remark:
totalCount=1000, nonNullCount=300
```

配置：

```text
strategyFamilies = [OD, PVD, RVD]
includedColumns = []
excludedColumns = []
includedStrategyTypes = []
excludedStrategyTypes = []
maxRvdColumnPairs = 4
maxStrategyCount = 20
```

生成结果分析：

1. `customer_id` 参与 OD 和 PVD。
2. `customer_id` 长度固定，所以不会生成 `PVD_LENGTH`。
3. `age` 是数值字段，会生成低频、标准差距离、四分位、字符集、长度、空值占位、类型格式等策略。
4. `email` 非数值字段，不满足数值距离和四分位条件，但会生成低频、字符集、长度、空值占位、类型格式。
5. `raw_payload` 可以生成 OD 和 PVD，但不会参与 RVD，因为类型包含 `binary`。
6. `remark` 不参与任何策略，因为 `detectable=false`。
7. RVD 候选字段会排除 `raw_payload` 和 `remark`，在 `customer_id`、`age`、`email` 之间生成有方向字段对，但最多生成 4 个。

可能的计划摘要如下：

```text
OD_LOW_FREQUENCY(customer_id)
PVD_CHARACTER_SET(customer_id)
PVD_NULL_PLACEHOLDER(customer_id)
PVD_TYPE_FORMAT(customer_id)

OD_LOW_FREQUENCY(age)
OD_NUMERIC_DISTANCE(age)
OD_QUANTILE(age)
PVD_CHARACTER_SET(age)
PVD_LENGTH(age)
PVD_NULL_PLACEHOLDER(age)
PVD_TYPE_FORMAT(age)

OD_LOW_FREQUENCY(email)
PVD_CHARACTER_SET(email)
PVD_LENGTH(email)
PVD_NULL_PLACEHOLDER(email)
PVD_TYPE_FORMAT(email)

RVD_ONE_TO_MANY(customer_id, age)
RVD_ONE_TO_MANY(customer_id, email)
RVD_ONE_TO_MANY(age, customer_id)
RVD_ONE_TO_MANY(age, email)
```

如果排序后总数超过 `maxStrategyCount`，只保留前 20 个。

## 十、需要注意的边界行为

1. `StrategyConfig.strategyFamilies` 如果为空，则 OD、PVD、RVD 都不会生成。
2. 空画像字段只可能生成 `PVD_NULL_PLACEHOLDER`，并且要求 `profile != null` 且 `totalCount > 0`；如果 `profile == null`，不会生成任何策略。
3. OD 低频策略几乎最宽松，只要字段有非空画像且策略类型未被过滤就会生成。
4. OD 数值距离策略要求标准差大于 0；如果字段所有数值都一样，不会生成。
5. OD 四分位策略要求 `Q3 > Q1`；如果四分位相等，不会生成。
6. RVD 是有方向的字段对，`A -> B` 和 `B -> A` 是两个不同计划。
7. RVD 字段对数量先受 `maxRvdColumnPairs` 限制，再和所有计划一起受 `maxStrategyCount` 限制。
8. 返回结果不可修改，但每个 `StrategyPlan` 内部也对字段列表和配置做了不可变包装。
9. 当前文件中的中文注释在终端显示为乱码，建议确认源文件实际编码是否为 UTF-8 无 BOM，避免后续维护时继续扩散乱码。

## 十一、一句话总结

`StrategyPlanGenerator` 的作用是把“数据画像 + 策略配置”转换成稳定、可排序、可重放、可过滤的策略执行计划，是画像阶段和策略执行阶段之间的计划生成层。
