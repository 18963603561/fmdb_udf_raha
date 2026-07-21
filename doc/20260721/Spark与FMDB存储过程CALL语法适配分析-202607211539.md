# Spark 与 FMDB 存储过程 CALL 语法适配分析

生成时间：2026-07-21 15:39

## 一、问题背景

用户提供的内网 FMDB 存储过程语法大致为：

```sql
CREATE [ OR REPLACE ] PROCEDURE [ IF NOT EXISTS ] procedure_identifier (PROCEDURE_ARGUMENT)
[ COMMENT procedure_comment ]
OPTIONS (
  procedure_option_list
)
LANGUAGE SQL|JAVA|SCALA
[BEGIN
  [DECLARE Block]
  code body
END]
```

当前需要判断：

1. Apache Spark 目前是否原生支持截图里的 `CREATE PROCEDURE ... LANGUAGE SQL|JAVA|SCALA ... BEGIN ... END` 语法。
2. 当前项目使用的 Spark 版本是否可以直接使用 `CALL`。
3. 如果 FMDB 支持 `CALL`，是否可以用 `CALL` 解决 `select udf(...)` 触发 Executor 侧嵌套 Spark 作业的问题。
4. 如果 `CALL` 可接受，Raha 三入口应该如何落地。

## 二、结论先行

当前项目 `pom.xml` 固定使用：

```xml
<spark.version>3.3.1</spark.version>
<scala.binary.version>2.12</scala.binary.version>
```

结论如下：

| 问题 | 结论 |
| --- | --- |
| 当前项目 Spark 3.3.1 是否支持截图里的 `CREATE PROCEDURE` | 不支持 |
| 当前项目 Spark 3.3.1 是否支持原生 `CALL procedure(...)` | 不支持 |
| Spark 最新版本是否完全支持截图里的 FMDB 过程语法 | 不完全支持 |
| Spark 4.x 是否有过程相关能力 | 有 `ProcedureCatalog`、`BoundProcedure`、`UnboundProcedure` 等接口，面向目录插件暴露过程给 `CALL` 使用 |
| `CALL` 是否可以解决 Raha 当前问题 | 可以，但前提是 `CALL` 被实现为 Driver 侧过程或平台服务过程 |
| 是否建议为了 `CALL` 直接升级 Spark 4.x | 不建议作为短期方案，升级牵涉 Java、Scala、Spark 运行环境和集群兼容性 |
| 如果 FMDB 内网已经支持存储过程 | 推荐优先用 FMDB 存储过程包装 Raha 三入口 |

一句话判断：

如果执行入口仍然是 Apache Spark 3.3.1 的 `spark-sql`，不能直接用截图里的 `CREATE PROCEDURE` 或 `CALL`；如果执行入口是 FMDB 自己的过程引擎，并且过程体在 Driver 或服务端控制层执行，那么 `CALL` 是可接受方案，而且比普通 `select udf(...)` 更适合 Raha 采集、训练、检测三类长流程。

## 三、官方 Spark 能力核对

### 1. Spark 3.3.1 不支持过程语法

当前项目实际依赖 Spark 3.3.1。核对官方 Spark 3.3.1 SQL 参考文档和 3.3.1 解析器源码后，结论是：

1. Spark 3.3.1 SQL 语法清单中有 `CREATE DATABASE`、`CREATE TABLE`、`CREATE VIEW`、`CREATE FUNCTION` 等语句，没有 `CREATE PROCEDURE`。
2. Spark 3.3.1 解析器 `SqlBaseParser.g4` 中检索不到 `CALL`。
3. Spark 3.3.1 解析器 `SqlBaseParser.g4` 中检索不到 `PROCEDURE`。
4. 本地 Maven 缓存中的 `spark-catalyst_2.12-3.3.1.jar` 解析器反编译信息没有 `CALL` 或 `PROCEDURE` token。
5. 本地 Maven 缓存中的 `spark-sql_2.12-3.3.1.jar` 没有 `ProcedureCatalog`、`BoundProcedure`、`UnboundProcedure` 等过程接口。

因此，在当前项目对应的 Spark 3.3.1 环境中，直接执行以下语句大概率会在解析阶段失败：

```sql
CREATE PROCEDURE raha_collect(...)
LANGUAGE JAVA
BEGIN
  ...
END;
```

```sql
CALL raha_collect(...);
```

这类失败不是 Raha 代码逻辑问题，而是 Spark 3.3.1 SQL Parser 不认识该语法。

### 2. Spark 4.x 有过程接口，但不是 FMDB 截图语法

Spark 最新官方文档显示，Spark 4.x 已有过程相关接口：

| 接口 | 作用 |
| --- | --- |
| `ProcedureCatalog` | 目录插件用于列出和加载过程 |
| `UnboundProcedure` | 尚未绑定输入类型的过程 |
| `BoundProcedure` | 绑定输入类型后的过程，执行 `call(input)` |

`BoundProcedure` 文档明确提到，Spark 会校验和重排 `CALL` 语句提供的参数，并且过程可以返回一个或多个结果集。

这说明 Spark 4.x 的方向是：

```text
Catalog 插件注册过程 -> SQL 使用 CALL 调用 -> 过程实现返回结果集
```

但这不等于 Spark 已经完整支持截图中的 FMDB 语法：

```sql
CREATE PROCEDURE ... LANGUAGE SQL|JAVA|SCALA ... BEGIN ... END
```

原因如下：

1. Spark 4.2 SQL Syntax 文档的 DDL 清单仍列出 `CREATE FUNCTION`，没有列出 `CREATE PROCEDURE`。
2. Spark 4.2 SQL Syntax 有 SQL Scripting，支持过程化控制语句，例如 `IF`、`LOOP`、`WHILE`、复合语句等，但这是 SQL 脚本能力，不等同于把过程定义持久化到 catalog 后再 `CALL`。
3. Spark 4.x 的过程接口是 `ProcedureCatalog` 扩展点，需要目录插件实现过程加载和执行，不是用户只写一段 `CREATE PROCEDURE ... LANGUAGE JAVA|SCALA` 就能落地。

所以更准确的说法是：

Spark 4.x 有 `CALL` 相关执行模型和过程扩展接口，但 Apache Spark 原生语法与 FMDB 截图中的内网存储过程语法不是同一套完整能力。

## 四、为什么 CALL 可以解决当前 Raha 问题

当前失败点来自普通 SQL UDF 的执行位置。

普通写法：

```sql
SELECT F_DW_DETCOLLECT('...');
```

执行路径通常是：

| 阶段 | 执行位置 | 说明 |
| --- | --- | --- |
| SQL 解析、优化、提交 Spark 作业 | Driver | `spark-sql` 或 `SparkSession.sql` 的 Driver 负责 |
| 扫描一行或多行输入 | Executor | Spark 创建 task |
| 计算 UDF 表达式 | Executor | `GenericUDF.evaluate` 在 task 线程中被调用 |
| UDF 内部再访问 SparkSession、执行 count、collect、write | Executor 内部尝试驱动 Spark | 触发 `SparkSession should only be created and accessed on the driver` |

这就是之前日志中出现失败任务的根因。

`CALL` 可以解决的原因不是 `CALL` 这个关键字天然特殊，而是因为“正确实现的存储过程”属于命令式入口，不再把 Raha 三函数当成 Executor 侧表达式执行。

理想 `CALL` 路径：

| 阶段 | 执行位置 | 说明 |
| --- | --- | --- |
| SQL 解析 `CALL P_DW_DETCOLLECT(...)` | FMDB SQL 前端或 Spark Driver | 识别为过程调用 |
| 调用过程实现 | Driver 或服务端控制层 | 直接进入 Java/Scala 过程体 |
| 过程体创建或获取 SparkSession | Driver | 合法 |
| 过程体执行 Raha 工作流 | Driver 提交 Spark 作业 | 合法 |
| 返回结果集 | Driver 汇总后返回 | 可与当前 UDF 返回结构保持一致 |

因此，`CALL` 解决的是“执行位置”问题：

把 Raha 从 Executor 表达式改成 Driver 侧过程命令。

如果 FMDB 的 `CALL` 最终又翻译成：

```sql
SELECT F_DW_DETCOLLECT('...');
```

那么它不能解决问题，因为还是会回到 Executor UDF。

## 五、FMDB 支持存储过程时的推荐落地

如果内网 FMDB 已经支持截图中的 `CREATE PROCEDURE` 和 `CALL`，建议优先采用下面方案。

### 1. 建三个过程入口

建议过程名与函数名区分，避免函数和过程同名带来的 catalog 冲突：

| 业务 | 推荐过程名 | 对应现有能力 |
| --- | --- | --- |
| 采集 | `P_DW_DETCOLLECT` | `RahaDetectionUdfService.collect(argument)` |
| 训练 | `P_DW_DETTRAIN` | `RahaDetectionUdfService.train(argument)` |
| 检测 | `P_DW_DETRUN` | `RahaDetectionUdfService.detect(argument)` |

如果 FMDB 确认函数和过程命名空间完全隔离，也可以继续沿用 `F_DW_DETCOLLECT`、`F_DW_DETTRAIN`、`F_DW_DETRUN` 作为过程名。但从兼容性和排障角度看，`P_` 前缀更稳。

### 2. 过程体不要再调用 SELECT UDF

不要这样包装：

```sql
CREATE PROCEDURE P_DW_DETCOLLECT(arg STRING)
LANGUAGE SQL
BEGIN
  SELECT inline(F_DW_DETCOLLECT(arg));
END;
```

这个写法只是把 `select udf(...)` 包了一层，如果底层仍由 Spark Executor 计算 UDF，问题不会消失。

推荐过程体直接调用 Java 或 Scala 服务：

```text
P_DW_DETCOLLECT
  -> 获取 Driver 侧 SparkSession
  -> RahaDetectionUdfService.create(spark)
  -> service.collect(argument)
  -> 转换为过程结果集
```

训练和检测同理：

```text
P_DW_DETTRAIN
  -> service.train(argument)
```

```text
P_DW_DETRUN
  -> service.detect(argument)
```

### 3. 返回结构保持稳定

当前三函数已经定义了稳定字段：

| 入口 | 当前返回字段数 | 建议过程返回 |
| --- | --- | --- |
| `F_DW_DETCOLLECT` | 31 | 保持 31 列，或返回一个 JSON 字符串列 |
| `F_DW_DETTRAIN` | 35 | 保持 35 列，或返回一个 JSON 字符串列 |
| `F_DW_DETRUN` | 29 | 保持 29 列，或返回一个 JSON 字符串列 |

如果 FMDB 存储过程支持返回表结构，推荐直接返回多列结果集，用户体验类似：

```sql
CALL P_DW_DETCOLLECT('...');
```

如果 FMDB 存储过程返回表结构配置复杂，可以短期返回 JSON：

```sql
CALL P_DW_DETCOLLECT('...');
```

结果列：

```text
result_json
```

这样最容易和当前 `RahaUdfDriverApp` 的 JSON 输出复用。

## 六、三种可选方案对比

| 方案 | 用户写法 | 是否解决 Executor UDF 问题 | 改造量 | 推荐级别 |
| --- | --- | --- | --- | --- |
| 继续普通 UDF | `SELECT F_DW_DETCOLLECT('...')` | 否 | 小 | 不推荐 |
| FMDB 存储过程 | `CALL P_DW_DETCOLLECT('...')` | 是，前提是过程体在 Driver 或服务端控制层执行 | 中 | 推荐 |
| Spark 3.3 自定义 SQL 扩展 | `CALL P_DW_DETCOLLECT('...')` | 是，前提是自定义 parser 和 command 在 Driver 执行 | 中高 | 可选 |
| 升级 Spark 4.x 并实现 `ProcedureCatalog` | `CALL catalog.namespace.proc(...)` | 是 | 高 | 长期可研 |
| UDF 代理加 Driver 服务 | `SELECT F_DW_DETCOLLECT('...')` | 是，前提是 UDF 只做远程提交或查询 | 高 | 保留 UDF 名称时推荐 |
| 保留 `RahaUdfDriverApp` | `spark-submit --class ... RahaUdfDriverApp collect ...` | 是 | 小 | 短期保底 |

## 七、为什么不建议短期升级 Spark 4.x

Spark 4.x 的过程接口对当前问题很有吸引力，但短期直接升级不划算。

原因如下：

1. 当前项目按 Java 8 编译，Spark 4.2 官方运行要求 Java 17、21 或 25。
2. Spark 4.0 以后 Scala 版本变为 2.13，当前项目依赖是 Scala 2.12。
3. Spark 4.x 的 `ProcedureCatalog` 需要额外实现 catalog 插件，不是只改 SQL 就能获得 FMDB 截图里的过程能力。
4. 线上 FMDB、Hive、Hadoop、YARN、Spark 运行包和用户提交方式都可能需要联调验证。
5. 当前 Raha 失败点已经可以通过 Driver 侧入口或 FMDB 过程入口规避，没有必要把短期需求押在大版本升级上。

## 八、建议的 FMDB 验证步骤

为了确认内网 FMDB 的 `CALL` 是否真的能解决问题，建议先做最小探针。

### 1. 语法探针

在 FMDB SQL 客户端执行一个不依赖 Raha 的简单过程：

```sql
CREATE OR REPLACE PROCEDURE P_RAHA_PING()
LANGUAGE SQL
BEGIN
  SELECT 1 AS ok;
END;
```

然后执行：

```sql
CALL P_RAHA_PING();
```

验收结果：

| 验收点 | 期望 |
| --- | --- |
| `CREATE PROCEDURE` | 成功 |
| `CALL` | 成功 |
| 返回值 | 一行一列，`ok = 1` |
| Spark SQL 侧 | 如果走 Spark 3.3.1 原生 parser，则这里会失败 |

### 2. 执行位置探针

建一个 Java 或 Scala 过程，过程体打印线程、Driver 标识和 SparkSession 获取结果。

验收结果：

| 验收点 | 期望 |
| --- | --- |
| 过程体能获取 Driver 侧 SparkSession | 是 |
| 过程体内执行 `spark.sql("select 1").collect()` | 成功 |
| 日志中不出现 Executor task 调用过程体 | 是 |
| 日志中不出现 `SparkSession should only be created and accessed on the driver` | 是 |

### 3. Raha 轻量探针

先不要跑完整采集、训练、检测，先让过程调用一个只返回参数和环境信息的 Java 方法，验证返回结构和异常传播。

验收通过后，再逐步替换为：

```text
RahaDetectionUdfService.collect(argument)
RahaDetectionUdfService.train(argument)
RahaDetectionUdfService.detect(argument)
```

## 九、推荐实施路径

### 短期

保留当前已经成功的 `RahaUdfDriverApp` 作为保底入口，同时验证 FMDB 存储过程能力。

短期目标不是马上替换所有 UDF，而是确认：

1. FMDB `CALL` 是否由 FMDB 过程引擎执行。
2. 过程体是否在 Driver 或服务端控制层执行。
3. 过程体能否直接调用当前 Raha Java 服务。
4. 过程能否返回用户需要的字段或 JSON。

### 中期

落地三个 FMDB 存储过程：

```sql
CALL P_DW_DETCOLLECT('...');
CALL P_DW_DETTRAIN('...');
CALL P_DW_DETRUN('...');
```

过程内部复用现有服务，不再走 `GenericUDF.evaluate`。

同时将对外说明从“只开放 UDF 函数”调整为“只开放 SQL 入口”，因为 `CALL` 本质是过程入口，不是函数入口。

### 长期

如果后续平台要求兼容 Apache Spark 原生生态，可以再评估两条路：

1. 基于 Spark 3.3.1 的 `SparkSessionExtensions` 实现自定义 parser 和 Driver 侧 command。
2. 升级 Spark 4.x 后实现 `ProcedureCatalog`，使用更接近原生的 `CALL` 机制。

长期方案需要单独评估 Spark、Scala、Java、FMDB、Hive、Hadoop 和部署链路兼容性。

## 十、最终建议

本项目当前最稳妥的选择是：

1. 不继续推动普通 `SELECT F_DW_DETCOLLECT('...')` 承载完整 Raha 工作流。
2. 如果 FMDB 已支持存储过程，则优先把 Raha 三入口改成 FMDB `CALL` 过程。
3. 过程体必须直接调用 Driver 侧 Java 或 Scala 服务，不能再包装 `SELECT udf(...)`。
4. 对外语法可以接受从 `SELECT udf(...)` 变更为 `CALL P_DW_DETCOLLECT(...)`。
5. 当前 `RahaUdfDriverApp` 继续保留，作为过程实现的代码参考和故障兜底入口。

这样可以同时满足：

1. 用户仍然只接触 SQL。
2. Raha 内部可以合法提交 Spark 作业。
3. 避免 Executor 侧访问 SparkSession 的失败。
4. 避免普通 UDF 被分布式 task 多次执行导致的幂等和资源风险。

## 十一、资料来源

1. Apache Spark 3.3.1 SQL Reference：`https://archive.apache.org/dist/spark/docs/3.3.1/sql-ref.html`
2. Apache Spark 3.3.1 `SqlBaseParser.g4`：`https://github.com/apache/spark/blob/v3.3.1/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4`
3. Apache Spark 4.2.0 SQL Syntax：`https://spark.apache.org/docs/latest/sql-ref-syntax.html`
4. Apache Spark 4.2.0 `ProcedureCatalog`：`https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/connector/catalog/ProcedureCatalog.html`
5. Apache Spark 4.2.0 `BoundProcedure`：`https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/connector/catalog/procedures/BoundProcedure.html`
6. Apache Spark 4.2.0 `UnboundProcedure`：`https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/connector/catalog/procedures/UnboundProcedure.html`
7. Apache Spark 4.2.0 Overview：`https://spark.apache.org/docs/latest/`
