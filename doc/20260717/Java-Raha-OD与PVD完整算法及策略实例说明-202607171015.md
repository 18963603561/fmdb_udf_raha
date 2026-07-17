# Java Raha OD 与 PVD 完整算法及策略实例说明

## 1. 文档目的

本文基于当前 Java Spark 工程源码和 hospital 数据集的实际执行产物，完整说明 OD、PVD 的算法、配置、策略展开方式、候选分数、实例计算和使用边界。

本文分析对象为当前工程实现，不将 Python Raha 中同名的 OD、PVD 直接视为等价算法。Python 与 Java 的差异见第 12 节。

## 2. 核心结论

当前 Java 工程包含 2 个策略族、7 类检测算法：

| 策略族 | 算法类型 | 主要用途 |
|---|---|---|
| OD | `OD_LOW_FREQUENCY` | 检测列内低频值 |
| OD | `OD_NUMERIC_DISTANCE` | 使用标准分数检测数值离群值 |
| OD | `OD_QUANTILE` | 使用四分位距检测数值离群值 |
| PVD | `PVD_CHARACTER_SET` | 检测少数字符组成模式 |
| PVD | `PVD_LENGTH` | 检测异常长度或少数长度 |
| PVD | `PVD_NULL_PLACEHOLDER` | 检测空值、空白值和占位值 |
| PVD | `PVD_TYPE_FORMAT` | 检测少数数据类型和格式违规 |

这里必须区分“算法类型”和“策略实例”：

```text
策略实例 = 算法 × 配置
配置 = 目标字段 + 固定阈值 + 字段画像参数
```

更完整地表示为：

```text
S = A × C
C = T + H + P
```

其中：

- `S`：一个可独立执行、可独立生成特征的策略实例。
- `A`：算法类型。
- `C`：该策略的完整配置。
- `T`：目标字段。
- `H`：全局阈值，例如少数比例、标准分数阈值。
- `P`：从目标字段画像得到的参数，例如均值、标准差、四分位数。

因此，同一个算法应用于不同字段，是不同策略；同一字段使用不同阈值，也是不同策略；字段画像改变导致参数改变，同样会形成不同策略标识。

## 3. 策略从生成到预测的完整链路

```text
读取数据
  -> 生成字段画像
  -> 根据字段条件选择适用算法
  -> 将字段画像参数和全局阈值固化为策略配置
  -> 为每个策略生成稳定策略标识
  -> Spark 执行策略并生成候选事件
  -> 将策略命中转换为模型特征
  -> 使用标签训练分类模型
  -> 模型输出最终错误预测
```

策略命中只是弱监督候选信号，不等于最终错误结论。一个单元格可以被多个策略命中，同一个 `PVD_TYPE_FORMAT` 策略也可能同时产生“类型不匹配”和“格式不匹配”两个候选事件。

策略标识由策略族、目标字段和配置哈希共同决定。配置键先按名称排序，再使用带长度的编码计算 `SHA-256`，因此配置顺序变化不会改变标识，实际参数变化会改变标识。

## 4. 默认配置

| 配置项 | 默认值 | 作用 |
|---|---:|---|
| `raha.strategy.od.low-frequency-ratio` | `0.01` | 低频值最大频率占非空记录数的比例 |
| `raha.strategy.od.minimum-numeric-count` | `3` | 生成数值距离策略所需的最小数值样本数 |
| `raha.strategy.od.z-threshold` | `3.0` | 标准分数命中阈值 |
| `raha.strategy.od.minimum-quantile-count` | `4` | 生成四分位策略所需的最小数值样本数 |
| `raha.strategy.od.iqr-multiplier` | `1.5` | 四分位距边界倍数 |
| `raha.strategy.pvd.minority-ratio` | `0.1` | 少数模式最大占比 |
| `raha.strategy.pvd.format-type` | `AUTO` | 根据字段名自动推断格式 |
| `raha.strategy.pvd.format-min-ratio` | `0.8` | 自动格式规则生效所需的最小匹配率 |
| `raha.strategy.pvd.placeholders` | `N/A|NULL|NONE|UNKNOWN|-|--|?` | 占位值集合 |

默认执行优先级依次为 `100`、`110`、`120`、`200`、`210`、`220`、`230`。优先级只影响执行顺序，不代表候选置信度。

## 5. OD 完整算法

### 5.1 低频值算法

#### 5.1.1 策略生成

每个启用且存在画像的业务字段生成一个低频值策略。

```text
maxFrequency = max(1, floor(nonNullCount × lowFrequencyRatio))
```

默认比例为 `0.01`。若字段有 1000 个非空值，则：

```text
maxFrequency = max(1, floor(1000 × 0.01)) = 10
```

对应策略可表示为：

```text
OD_LOW_FREQUENCY
× targetColumn=sample
× maxFrequency=10
```

hospital 实测规范配置为：

```text
OD|sample|maxFrequency=10|strategyType=OD_LOW_FREQUENCY
```

#### 5.1.2 候选判断

先按值哈希分组计数。实际值频率 `f` 满足以下条件时命中：

```text
f <= maxFrequency
```

空值不参与低频统计。相同原始值通过相同值哈希聚合，驱动端不需要收集原始值。

#### 5.1.3 候选分数

```text
score = limit((maxFrequency - f + 1) / maxFrequency, 0, 1)
```

当阈值为 10 时：

| 值频率 | 分数 |
|---:|---:|
| 1 | 1.0 |
| 5 | 0.6 |
| 10 | 0.1 |
| 11 | 不命中 |

#### 5.1.4 实测边界

hospital 的 `sample` 字段由大量不同的样本描述组成，该策略命中 704 个候选。坐标 `1000|sample` 的值为 `25 patients`，干净数据也是 `25 patients`，因此它是合法低频值，不是实际错误。

结论：低频算法召回范围大，但对标识符、地域值、自由文本和天然长尾字段容易产生误报，必须作为模型特征使用，不能直接输出为最终错误。

### 5.2 数值距离算法

#### 5.2.1 策略生成

只有同时满足以下条件的字段才生成策略：

- 数值样本数不少于 `minimumNumericCount`，默认值为 3。
- 字段均值不为空。
- 字段总体标准差不为空且大于 0。

每个策略固化字段画像中的均值 `mean`、总体标准差 `standardDeviation` 和阈值 `zThreshold`。

hospital 的 `zip` 策略为：

```text
OD_NUMERIC_DISTANCE
× targetColumn=zip
× numericMean=37224.93505154639
× numericStandardDeviation=9051.23724554622
× zThreshold=3
```

#### 5.2.2 候选判断

只有符合数值正则表达式的非空文本参与计算。标准距离为：

```text
z = abs(x - mean) / standardDeviation
```

使用严格大于判断：

```text
z > zThreshold
```

#### 5.2.3 候选分数

```text
score = limit(z / (zThreshold × 2), 0, 1)
```

#### 5.2.4 hospital 实例

坐标 `433|zip` 的值为 `99508`：

```text
z = abs(99508 - 37224.93505154639) / 9051.23724554622
  = 6.8812

6.8812 > 3，因此命中
score = limit(6.8812 / 6, 0, 1) = 1.0
```

但该坐标在干净数据中仍为 `99508`，它是合法的阿拉斯加邮政编码。这个实例说明：把邮政编码、电话号码、业务编号当作连续数值，会把合法地域差异或编码空间差异误判为数值离群。

### 5.3 四分位距算法

#### 5.3.1 策略生成

只有同时满足以下条件的字段才生成策略：

- 数值样本数不少于 `minimumQuantileCount`，默认值为 4。
- 第一四分位数和第三四分位数均存在。
- 第三四分位数严格大于第一四分位数。

策略固化 `Q1`、`Q3` 和倍数 `k`，默认 `k=1.5`。

#### 5.3.2 候选判断

```text
IQR = Q3 - Q1
lowerBound = Q1 - k × IQR
upperBound = Q3 + k × IQR
```

只有以下情况命中：

```text
x < lowerBound 或 x > upperBound
```

等于边界时不命中。

#### 5.3.3 候选分数

```text
outsideDistance = lowerBound - x，x 低于下界时
outsideDistance = x - upperBound，x 高于上界时
score = limit(outsideDistance / (IQR × 3), 0, 1)
```

#### 5.3.4 hospital 实例

`provider_number` 的实测配置为：

```text
Q1 = 10015
Q3 = 10044
k = 1.5
IQR = 29
lowerBound = 9971.5
upperBound = 10087.5
```

坐标 `393|provider_number` 的值为 `10108`：

```text
10108 > 10087.5，因此命中
outsideDistance = 20.5
score = 20.5 / (29 × 3) = 0.2356
```

干净数据中的值仍为 `10108`，所以这也是合法编号被统计边界命中的实例。对编号字段使用四分位距，需要结合字段语义禁用或降低权重。

## 6. PVD 完整算法

### 6.1 字符集模式算法

#### 6.1.1 字符签名

每个非空、非空白值按是否包含以下字符类别生成签名：

| 标记 | 含义 |
|---|---|
| `D` | 包含数字 |
| `L` | 包含拉丁字母 |
| `C` | 包含中文字符 |
| `S` | 包含空白字符 |
| `P` | 包含非字母、非数字、非空白的符号 |

标记按 `D`、`L`、`C`、`S`、`P` 固定顺序拼接。例如：

| 值 | 签名 |
|---|---|
| `3344933541` | `D` |
| `3344x33541` | `DL` |
| `AB-100` | `DLP` |
| `北京 01` | `DCS` |

#### 6.1.2 候选判断

先统计每种签名的数量，数量最大的签名为主导签名；数量并列时使用字典序较小的签名，保证结果稳定。

签名数量为 `count`、有效值总数为 `total`、主导签名数量为 `dominantCount` 时，必须同时满足：

```text
count < dominantCount
count / total <= minorityRatio
```

默认 `minorityRatio=0.1`。

候选分数为：

```text
score = limit(1 - count / total, 0, 1)
```

例如，1000 个电话号码中 970 个签名为 `D`，30 个签名为 `DL`：

```text
30 < 970
30 / 1000 = 0.03 <= 0.1
```

因此 30 个 `DL` 值全部命中，分数为 `0.97`。

#### 6.1.3 hospital 实例

`phone` 中的 `102|phone=3344x33541` 含数字和拉丁字母，签名为 `DL`；正常电话号码通常为 `D`。该值的干净值为 `3344933541`，字符集策略能够捕获此类字符替换污染。

### 6.2 长度异常算法

#### 6.2.1 策略生成

只有字段画像满足 `maxLength > minLength` 时才生成。所有非空值长度相同的字段不生成该策略。

#### 6.2.2 候选判断

对非空、非空白值计算字符串长度，并使用精确相对误差 `0.0` 计算长度的 `Q1` 和 `Q3`：

```text
IQR = Q3 - Q1
lowerBound = Q1 - iqrMultiplier × IQR
upperBound = Q3 + iqrMultiplier × IQR
```

候选条件是以下两类条件的并集：

```text
条件一：IQR > 0，并且 length < lowerBound 或 length > upperBound
条件二：该长度不是主导长度，并且该长度占比 <= minorityRatio
```

所有命中候选的当前分数固定为 `1.0`。

例如，长度 10 出现 900 次、长度 12 出现 80 次、长度 40 出现 20 次，且 `IQR=0`。虽然四分位边界条件停用，但长度 12 和长度 40 均不是主导长度，占比分别为 `0.08` 和 `0.02`，都会被少数长度条件命中。

#### 6.2.3 hospital 实测风险

`measure_name`、`name`、`address_1` 等自然长度变化很大的字段会产生大量候选。hospital 中该算法的 12 个策略合计产生 3391 个候选事件，是 OD 与 PVD 候选总量的主要来源。

这不表示 3391 个单元格都是错误。自然语言和地址字段本身具有宽长度分布，固定分数 `1.0` 会放大噪声，生产配置应支持按字段禁用、提高少数长度限制，或者将分数改为随边界距离和频率连续变化。

### 6.3 空值与占位值算法

#### 6.3.1 策略生成

字段总记录数大于 0 时生成。默认配置在载入后转换为：

```text
placeholders=N/A,NULL,NONE,UNKNOWN,-,--,?
```

#### 6.3.2 候选判断

| 情况 | 原因编码 | 分数 |
|---|---|---:|
| 原始值为真正空值 | `PVD_NULL_VALUE` | 1.0 |
| 原始值非空，但去除首尾空白后为空字符串 | `PVD_BLANK_VALUE` | 1.0 |
| 去除首尾空白并转大写后属于占位值集合 | `PVD_PLACEHOLDER_VALUE` | 1.0 |

占位值匹配不区分大小写，但不会自动识别配置集合之外的业务缺失标记。

#### 6.3.3 hospital 实测结果

hospital 的 19 个该类策略均为 0 命中，原因是数据使用字面量 `empty`，而默认占位集合不包含 `EMPTY`。

不能仅为提高召回直接加入 `EMPTY`。该数据的干净文件中也保留大量 `empty`，说明它在此基准中可能表示允许的缺失状态。是否将其视为错误，必须由字段级业务规则决定。

### 6.4 类型与格式算法

该算法在一个策略中执行两条相互独立的检测支路，因此一个单元格可能产生两个候选事件。

#### 6.4.1 类型分类

类型按以下顺序匹配，先匹配成功者生效：

| 类型 | 识别规则 |
|---|---|
| `INTEGER` | 可带正负号的整数 |
| `DECIMAL` | 工程统一数值正则能够识别的非整数数值 |
| `LATIN` | 仅包含拉丁字母 |
| `CHINESE` | 仅包含中文字符 |
| `ALPHANUMERIC` | 仅包含任意语言字母和数字 |
| `MIXED` | 以上均不满足 |

类型候选判断和字符集少数模式一致：实际类型不是主导类型，且实际类型占比不超过 `minorityRatio`。类型候选分数为：

```text
score = limit(1 - typeCount / total, 0, 1)
```

#### 6.4.2 格式推断

当 `formatType=AUTO` 时，根据字段名推断格式：

| 字段名特征 | 推断格式 |
|---|---|
| 包含 `email` 或 `mail` | `EMAIL` |
| 包含 `phone`、`mobile` 或 `tel` | `PHONE` |
| 包含 `date` 或以 `day` 结尾 | `DATE` |
| 包含 `time` | `TIME` |
| 以 `id`、`code`、`number` 或 `no` 结尾 | `IDENTIFIER` |
| 其他字段名 | 不执行格式支路 |

当前支持的格式规则为：

| 格式 | 规则概要 |
|---|---|
| `DATE` | 年月日或月日年，分隔符为横线或斜线 |
| `TIME` | 24 小时制时分，可带秒 |
| `PHONE` | 可带加号，首字符为数字，后续允许数字、括号、空格和横线 |
| `EMAIL` | 常规邮箱地址结构 |
| `IDENTIFIER` | 1 至 20 个字母，可带一个横线或下划线，再接 1 至 30 个数字 |

自动推断格式后，先计算整列格式匹配率：

```text
matchedRatio = matchedCount / total
```

只有 `matchedRatio >= formatMinRatio` 时规则才生效，默认阈值为 `0.8`。所有不匹配值生成格式候选，其分数为：

```text
score = limit(max(0.5, matchedRatio), 0, 1)
```

若显式指定格式而不是使用 `AUTO`，则不经过最低匹配率保护。

#### 6.4.3 hospital 实例

`102|phone=3344x33541`：

- 类型为 `ALPHANUMERIC`，而电话号码主导类型通常为 `INTEGER`，可产生类型不匹配候选。
- 字符 `x` 不符合 `PHONE` 格式，可产生格式不匹配候选。
- 同一值还可能被字符集策略识别为少数 `DL` 签名。
- 干净值为 `3344933541`，因此这些策略在该实例上提供了相互增强的有效信号。

实测对齐文件中 `102|phone` 出现两次，正是同一 `PVD_TYPE_FORMAT` 策略分别输出类型和格式候选，不是数据重复。

## 7. hospital 策略展开实例

hospital 有 19 个参与检测的业务字段，索引字段不作为检测目标。

### 7.1 OD 展开数量

```text
OD 策略数
= 低频值适用字段数
+ 数值距离适用字段数
+ 四分位距适用字段数
= 19 + 3 + 3
= 25
```

数值距离和四分位距只在 `provider_number`、`phone`、`zip` 三个数值画像有效字段上生成。

### 7.2 PVD 展开数量

```text
PVD 策略数
= 字符集适用字段数
+ 长度策略适用字段数
+ 空值占位策略适用字段数
+ 类型格式适用字段数
= 19 + 12 + 19 + 19
= 69
```

长度策略只在最小长度和最大长度不同的 12 个字段上生成。

### 7.3 总数

```text
OD 与 PVD 策略总数 = 25 + 69 = 94
```

这就是“7 类算法”展开成“94 个策略”的实际例子。

## 8. hospital 实测候选分布

数据来源为 OD、PVD 独立执行产物 `output-odpvd/java-strategy-alignment.jsonl`。

| 算法类型 | 策略数 | 有命中的策略数 | 候选事件数 |
|---|---:|---:|---:|
| `OD_LOW_FREQUENCY` | 19 | 17 | 1407 |
| `OD_NUMERIC_DISTANCE` | 3 | 3 | 60 |
| `OD_QUANTILE` | 3 | 3 | 99 |
| `PVD_CHARACTER_SET` | 19 | 14 | 372 |
| `PVD_LENGTH` | 12 | 11 | 3391 |
| `PVD_NULL_PLACEHOLDER` | 19 | 0 | 0 |
| `PVD_TYPE_FORMAT` | 19 | 11 | 272 |
| 合计 | 94 | 59 | 5601 |

这里统计的是候选事件数，不是去重后的错误单元格数。同一坐标可被多个策略命中，也可被类型格式策略的两个支路重复命中。

## 9. 典型策略的完整配置实例

| 策略实例 | 完整含义 |
|---|---|
| `OD|sample|maxFrequency=10|strategyType=OD_LOW_FREQUENCY` | 对 `sample` 执行低频算法，频率不超过 10 时命中 |
| `OD|zip|numericMean=37224.93505154639|numericStandardDeviation=9051.23724554622|strategyType=OD_NUMERIC_DISTANCE|zThreshold=3` | 对 `zip` 使用已固化的均值、标准差和 3 倍阈值执行数值距离检测 |
| `OD|provider_number|iqrMultiplier=1.5|numericQ1=10015|numericQ3=10044|strategyType=OD_QUANTILE` | 对 `provider_number` 使用已固化四分位数执行四分位距检测 |
| `PVD|measure_name|minorityRatio=0.1|strategyType=PVD_CHARACTER_SET` | 对 `measure_name` 检测占比不超过 10% 的少数字符签名 |
| `PVD|measure_name|iqrMultiplier=1.5|minorityRatio=0.1|strategyType=PVD_LENGTH` | 对 `measure_name` 同时使用长度四分位边界和少数长度规则 |
| `PVD|provider_number|placeholders=N/A,NULL,NONE,UNKNOWN,-,--,?|strategyType=PVD_NULL_PLACEHOLDER` | 对 `provider_number` 检测空值、空白和默认占位值 |
| `PVD|phone|formatMinRatio=0.8|formatType=AUTO|minorityRatio=0.1|strategyType=PVD_TYPE_FORMAT` | 对 `phone` 检测少数类型，并在电话格式匹配率至少为 80% 时检测格式违规 |

## 10. 算法适用性和主要风险

| 算法 | 适合字段 | 主要风险 | 建议 |
|---|---|---|---|
| 低频值 | 低基数类别、状态码 | 合法长尾值被大量命中 | 对自由文本、编号和高基数字段禁用或降低权重 |
| 数值距离 | 连续测量值 | 编码、邮编、电话被错误解释为连续数值 | 增加字段语义类型，排除标识符类字段 |
| 四分位距 | 连续且偏态可控的数值 | 长尾分布和编号产生合法极值 | 按字段配置倍数，并排除标识符 |
| 字符集模式 | 电话、编码、纯文本类型字段 | 合法混合字符可能是少数模式 | 结合字段格式和标签训练 |
| 长度异常 | 固定长度编码、结构化号码 | 自然语言和地址产生大量噪声 | 对自由文本禁用，分数改为连续值 |
| 空值占位 | 明确禁止缺失的字段 | 业务允许的占位值被误报 | 使用字段级占位规则，不使用全表统一结论 |
| 类型格式 | 电话、邮箱、日期、标识符 | 字段名推断错误或格式规则覆盖不足 | 支持显式字段语义和字段级格式配置 |

## 11. 参数调整建议

### 11.1 必须优先增加字段语义

当前 `provider_number`、`zip`、`phone` 能被解析为数字，因此会生成数值距离和四分位策略，但它们本质上是标识符或编码，不是可计算距离的连续测量值。

建议新增字段角色：

```text
CONTINUOUS_NUMBER：允许数值距离和四分位策略
IDENTIFIER：禁止数值距离和四分位策略，允许格式和字符模式策略
FREE_TEXT：禁止低频值和长度少数模式，保留受控字符模式
CATEGORY：允许低频值和少数模式
```

### 11.2 支持字段级配置

全局阈值不能覆盖不同字段的统计特征。建议支持以下形式的字段级覆盖：

```properties
raha.strategy.column.phone.disable=OD_NUMERIC_DISTANCE,OD_QUANTILE
raha.strategy.column.measure_name.disable=OD_LOW_FREQUENCY,PVD_LENGTH
raha.strategy.column.zip.format-type=ZIP_CODE
raha.strategy.column.address_2.placeholders=EMPTY,N/A,NULL
```

以上是建议配置形式，当前工程尚未实现，不能直接作为现有配置使用。

### 11.3 调整长度候选分数

当前所有长度候选分数固定为 `1.0`，无法区分轻微少数长度和极端长度。建议分为两部分：

```text
distanceScore = 超出边界的距离 / 标准化长度尺度
rarityScore = 1 - lengthCount / total
score = max(distanceScore, rarityScore)
```

并将结果限制在 0 到 1。这样可以降低自然长度波动对模型的干扰。

### 11.4 保留多策略融合

`3344x33541` 同时被字符集、类型和格式分支命中，是有价值的多证据一致性；`99508` 主要被数值类策略命中，却缺少字符和格式异常证据，更可能是合法极值。

因此应让分类模型学习策略组合，不应把单策略命中直接转成最终错误。

## 12. 与 Python Raha 的同名算法差异

Java 和 Python 都使用 OD、PVD 名称，但策略定义并不相同。

### 12.1 Python OD

Python OD 调用 `dBoost`，默认展开：

```text
histogram：5 × 5 = 25 组参数
gaussian：1.0 到 3.0，步长 0.25，共 9 组参数
OD 合计：34 组表级配置
```

Python 的 OD 策略是外部 `dBoost` 的整表参数组合；Java 的 OD 是按字段画像生成的低频、标准分数和四分位距策略。两者不能仅按策略名称或策略数量一一对应。

### 12.2 Python PVD

Python PVD 针对每个字段中实际出现过的每个字符生成一个配置，再检测单元格是否包含该字符。因此：

```text
Python PVD 策略数 = 各字段不同字符数量之和
```

Java PVD 则固定为字符类别签名、长度、空值占位、类型格式四类算法，再按字段适用条件展开。因此 Python 策略数量通常明显更多，但每个策略更细；Java 策略数量较少，但单策略语义更聚合。

### 12.3 不能直接比较策略总数

策略数量取决于策略粒度：

- Python 的一个字符就是一个 PVD 配置。
- Java 的数字、字母、中文、空白、符号组合形成一个字符签名策略。
- Python OD 的一组 `dBoost` 参数对整表执行。
- Java OD 的均值、标准差和四分位数按字段固化。

因此“Python 策略更多”不代表算法更完整，“Java 策略更少”也不代表检测能力必然更弱。有效比较应使用最终精确率、召回率、F1、耗时、候选覆盖和误报类型。

## 13. 源码与实测依据

| 内容 | 工程位置 |
|---|---|
| 默认参数 | `src/main/resources/raha-defaults.properties` |
| 策略生成条件 | `src/main/java/com/fiberhome/ml/raha/strategy/StrategyPlanGenerator.java` |
| 策略稳定标识 | `src/main/java/com/fiberhome/ml/raha/strategy/StrategyIdentityGenerator.java` |
| 低频值算法 | `src/main/java/com/fiberhome/ml/raha/strategy/od/LowFrequencyStrategy.java` |
| 数值距离算法 | `src/main/java/com/fiberhome/ml/raha/strategy/od/NumericDistanceStrategy.java` |
| 四分位距算法 | `src/main/java/com/fiberhome/ml/raha/strategy/od/QuantileOutlierStrategy.java` |
| 字符集算法 | `src/main/java/com/fiberhome/ml/raha/strategy/pvd/CharacterSetStrategy.java` |
| 长度算法 | `src/main/java/com/fiberhome/ml/raha/strategy/pvd/LengthAnomalyStrategy.java` |
| 空值占位算法 | `src/main/java/com/fiberhome/ml/raha/strategy/pvd/NullPlaceholderStrategy.java` |
| 类型格式算法 | `src/main/java/com/fiberhome/ml/raha/strategy/pvd/TypeFormatStrategy.java` |
| Java hospital 实测对齐产物 | `F:/ai-code/fmdb_udf_schmatch/data/raha-hospital-comparison-20260716/output-odpvd/java-strategy-alignment.jsonl` |
| hospital 脏数据和干净数据 | `datasets/hospital/dirty.csv`、`datasets/hospital/clean.csv` |
| Python 策略实现 | `F:/ai-code/raha/raha-master/raha/detection.py` |

## 14. 最终结论

当前 Java 的 OD、PVD 不是 2 个单一规则，而是 7 类算法按字段和配置展开的策略系统。hospital 的 19 个业务字段最终生成 94 个策略，公式为：

```text
策略 = 算法 × 配置
配置 = 目标字段 + 阈值 + 字段画像参数
```

OD 擅长提供低频和数值离群信号，但当前缺少字段语义，容易误判邮编、电话和业务编号。PVD 能有效发现字符污染和格式违规，但长度策略在自然文本上候选过多，空值占位策略又受业务占位集合影响明显。

生产落地的关键不是盲目增加策略数，而是补充字段语义、字段级启停和字段级参数，让每个算法只在适合的字段上执行，并继续通过模型融合多个弱策略信号得到最终预测。
