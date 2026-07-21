package com.fiberhome.ml.raha.strategy.execution;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.md5;
import static org.apache.spark.sql.functions.trim;

/**
 * 将同一批次的一对多关系策略合并为共享长表、关联和聚合作业。
 */
public final class RvdBatchStrategyExecutor {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RvdBatchStrategyExecutor.class);
    /** 单次 Spark 作业允许处理的最大 RVD 计划数，限制驱动端瞬时结果规模。 */
    private static final int DEFAULT_MAX_PLANS_PER_BATCH = 16;
    /** 提供可测试完成时间的时钟。 */
    private final Clock clock;
    /** 单次 Spark 作业允许处理的最大 RVD 计划数。 */
    private final int maxPlansPerBatch;

    public RvdBatchStrategyExecutor(Clock clock) {
        this(clock, DEFAULT_MAX_PLANS_PER_BATCH);
    }

    RvdBatchStrategyExecutor(Clock clock, int maxPlansPerBatch) {
        if (clock == null) {
            throw new IllegalArgumentException("RVD 批量执行器时钟不能为空");
        }
        if (maxPlansPerBatch <= 0) {
            throw new IllegalArgumentException("RVD 单批计划数必须大于 0");
        }
        this.clock = clock;
        this.maxPlansPerBatch = maxPlansPerBatch;
    }

    /**
     * 批量执行一组一对多关系策略，批次失败时所有策略返回失败摘要。
     *
     * @param jobId 任务标识
     * @param stageId 阶段标识
     * @param dataset 只读输入数据集
     * @param plans 一对多关系策略计划
     * @param timeoutMillis 批次超时时间
     * @return 按输入计划顺序保存的策略执行结果
     */
    public List<StrategyExecutionResult> execute(String jobId,
                                                  String stageId,
                                                  RahaDataset dataset,
                                                  List<StrategyPlan> plans,
                                                  long timeoutMillis) {
        validate(jobId, stageId, dataset, plans, timeoutMillis);
        if (plans.isEmpty()) {
            return Collections.emptyList();
        }
        long batchStartNanos = System.nanoTime();
        List<StrategyExecutionResult> results = new ArrayList<StrategyExecutionResult>(
                plans.size());
        int batchCount = (plans.size() + maxPlansPerBatch - 1) / maxPlansPerBatch;
        LOGGER.info("开始分批执行 RVD 策略，jobId={}，stageId={}，planCount={}，"
                        + "maxPlansPerBatch={}，batchCount={}，timeoutMillis={}",
                jobId, stageId, plans.size(), maxPlansPerBatch, batchCount, timeoutMillis);
        for (int start = 0, batchIndex = 0; start < plans.size();
                start += maxPlansPerBatch, batchIndex++) {
            int end = Math.min(start + maxPlansPerBatch, plans.size());
            List<StrategyPlan> currentPlans = plans.subList(start, end);
            long remainingMillis = timeoutMillis - elapsedMillis(batchStartNanos);
            if (remainingMillis <= 0L) {
                // 总超时耗尽后不再提交 Spark 作业，剩余计划统一返回超时结果。
                results.addAll(failed(jobId, stageId, dataset, currentPlans,
                        elapsedMillis(batchStartNanos), "STRATEGY_TIMEOUT",
                        "RVD 分批执行超过总超时时间"));
                continue;
            }
            results.addAll(executeBatch(jobId, stageId, dataset, currentPlans,
                    remainingMillis, batchIndex, batchCount));
        }
        LOGGER.info("RVD 策略分批执行结束，jobId={}，stageId={}，planCount={}，"
                        + "batchCount={}，runtimeMillis={}",
                jobId, stageId, plans.size(), batchCount, elapsedMillis(batchStartNanos));
        return results;
    }

    private List<StrategyExecutionResult> executeBatch(String jobId,
                                                       String stageId,
                                                       RahaDataset dataset,
                                                       List<StrategyPlan> plans,
                                                       long timeoutMillis,
                                                       int batchIndex,
                                                       int batchCount) {
        long startNanos = System.nanoTime();
        String jobGroup = jobId + ":rvd-batch:" + stageId + ":" + batchIndex;
        ExecutorService worker = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "raha-rvd-batch");
            thread.setDaemon(true);
            return thread;
        });
        Future<List<Row>> future = worker.submit(new Callable<List<Row>>() {
            @Override
            public List<Row> call() {
                SparkSession spark = dataset.getDataFrame().sparkSession();
                spark.sparkContext().setJobGroup(jobGroup,
                        "Raha RVD batch " + stageId, true);
                try {
                    return detect(dataset, plans);
                } finally {
                    spark.sparkContext().clearJobGroup();
                }
            }
        });
        LOGGER.info("开始执行 RVD 子批次，jobId={}，stageId={}，batchIndex={}，"
                        + "batchCount={}，planCount={}，timeoutMillis={}",
                jobId, stageId, batchIndex + 1, batchCount, plans.size(), timeoutMillis);
        try {
            List<Row> rows = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            long runtimeMillis = elapsedMillis(startNanos);
            List<StrategyExecutionResult> results = succeeded(
                    jobId, stageId, dataset, plans, rows, runtimeMillis);
            LOGGER.info("RVD 子批次执行完成，jobId={}，stageId={}，batchIndex={}，"
                            + "batchCount={}，planCount={}，candidateRowCount={}，runtimeMillis={}",
                    jobId, stageId, batchIndex + 1, batchCount, plans.size(), rows.size(),
                    runtimeMillis);
            return results;
        } catch (TimeoutException exception) {
            future.cancel(true);
            cancelSparkJob(dataset, jobGroup);
            LOGGER.warn("RVD 策略批量执行超时，jobId={}，stageId={}，timeoutMillis={}",
                    jobId, stageId, timeoutMillis);
            return failed(jobId, stageId, dataset, plans,
                    elapsedMillis(startNanos), "STRATEGY_TIMEOUT", "RVD 批量执行超过超时时间");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            cancelSparkJob(dataset, jobGroup);
            LOGGER.error("RVD 策略批量执行线程被中断，jobId={}，stageId={}",
                    jobId, stageId, exception);
            return failed(jobId, stageId, dataset, plans,
                    elapsedMillis(startNanos), "STRATEGY_INTERRUPTED", "RVD 批量执行线程被中断");
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause() == null ? exception : exception.getCause();
            LOGGER.error("RVD 策略批量执行失败，jobId={}，stageId={}",
                    jobId, stageId, cause);
            return failed(jobId, stageId, dataset, plans,
                    elapsedMillis(startNanos), "STRATEGY_EXECUTION_FAILED", safeMessage(cause));
        } finally {
            worker.shutdownNow();
        }
    }

    private static List<Row> detect(RahaDataset dataset,
                                    List<StrategyPlan> plans) {
        Dataset<Row> values = longValues(dataset).persist(StorageLevel.MEMORY_AND_DISK());
        try {
            Dataset<Row> pairFrame = pairFrame(dataset.getDataFrame().sparkSession(), plans);
            Dataset<Row> leftValues = values.alias("l");
            Dataset<Row> configuredLeft = broadcast(pairFrame.alias("p"))
                    .join(leftValues,
                            col("p.left_column").equalTo(col("l.column_name")))
                    .select(col("p.strategy_id").alias("strategy_id"),
                            col("p.left_column").alias("left_column"),
                            col("p.right_column").alias("right_column"),
                            col("l.row_id").alias("row_id"),
                            col("l.value_hash").alias("left_hash"));
            Dataset<Row> rightValues = values.alias("r");
            Dataset<Row> pairValues = configuredLeft.alias("c")
                    .join(rightValues,
                            col("c.row_id").equalTo(col("r.row_id"))
                                    .and(col("c.right_column")
                                            .equalTo(col("r.column_name"))))
                    .select(col("c.strategy_id").alias("strategy_id"),
                            col("c.left_column").alias("left_column"),
                            col("c.right_column").alias("right_column"),
                            col("c.row_id").alias("row_id"),
                            col("c.left_hash").alias("left_hash"),
                            col("r.value_hash").alias("right_hash"));
            Dataset<Row> conflicts = pairValues.groupBy(
                            "strategy_id", "left_column", "right_column", "left_hash")
                    .agg(countDistinct("right_hash").alias("distinct_right_count"),
                            count(lit(1)).alias("group_size"))
                    .filter(col("distinct_right_count").gt(1L));
            Dataset<Row> candidates = pairValues.alias("v").join(conflicts.alias("g"),
                            col("v.strategy_id").equalTo(col("g.strategy_id"))
                                    .and(col("v.left_column").equalTo(col("g.left_column")))
                                    .and(col("v.right_column").equalTo(col("g.right_column")))
                                    .and(col("v.left_hash").equalTo(col("g.left_hash"))))
                    .select(col("v.strategy_id").alias("strategy_id"),
                            col("v.left_column").alias("left_column"),
                            col("v.right_column").alias("right_column"),
                            col("v.row_id").alias("row_id"),
                            col("v.left_hash").alias("left_hash"),
                            col("v.right_hash").alias("right_hash"),
                            col("g.distinct_right_count").alias("distinct_right_count"),
                            col("g.group_size").alias("group_size"));
            // 当前仓储契约按策略保存命中列表，因此批量结果只在最终边界收集一次。
            return candidates.collectAsList();
        } finally {
            values.unpersist(false);
        }
    }

    private static Dataset<Row> longValues(RahaDataset dataset) {
        List<Column> entries = new ArrayList<Column>();
        for (ColumnMetadata column : dataset.getColumns()) {
            if (column.isDetectable()) {
                entries.add(org.apache.spark.sql.functions.struct(
                        lit(column.getName()).alias("column_name"),
                        SparkStrategySupport.quotedColumn(column.getName())
                                .cast("string").alias("value_text")));
            }
        }
        if (entries.isEmpty()) {
            throw new IllegalArgumentException("RVD 批量执行缺少可检测字段");
        }
        Dataset<Row> exploded = dataset.getDataFrame().select(
                SparkStrategySupport.quotedColumn(dataset.getRowIdColumn())
                        .cast("string").alias("row_id"),
                explode(org.apache.spark.sql.functions.array(
                        entries.toArray(new Column[entries.size()]))).alias("entry"));
        return exploded.select(col("row_id"),
                        col("entry.column_name").alias("column_name"),
                        col("entry.value_text").alias("value_text"))
                .filter(col("value_text").isNotNull()
                        .and(trim(col("value_text")).notEqual("")))
                .withColumn("value_hash", md5(col("value_text")))
                .select("row_id", "column_name", "value_hash");
    }

    private static Dataset<Row> pairFrame(SparkSession spark,
                                          List<StrategyPlan> plans) {
        List<Row> rows = new ArrayList<Row>(plans.size());
        for (StrategyPlan plan : plans) {
            rows.add(RowFactory.create(plan.getStrategyId(),
                    SparkStrategySupport.required(plan, StrategyConfigurationKeys.LEFT_COLUMN),
                    SparkStrategySupport.required(plan, StrategyConfigurationKeys.RIGHT_COLUMN)));
        }
        StructType schema = new StructType()
                .add("strategy_id", DataTypes.StringType, false)
                .add("left_column", DataTypes.StringType, false)
                .add("right_column", DataTypes.StringType, false);
        return spark.createDataFrame(rows, schema);
    }

    private List<StrategyExecutionResult> succeeded(String jobId,
                                                     String stageId,
                                                     RahaDataset dataset,
                                                     List<StrategyPlan> plans,
                                                     List<Row> rows,
                                                     long runtimeMillis) {
        Map<String, List<StrategyHit>> hitsByStrategy =
                new LinkedHashMap<String, List<StrategyHit>>();
        for (StrategyPlan plan : plans) {
            hitsByStrategy.put(plan.getStrategyId(), new ArrayList<StrategyHit>());
        }
        for (Row row : rows) {
            String strategyId = row.getAs("strategy_id");
            List<StrategyHit> hits = hitsByStrategy.get(strategyId);
            if (hits == null) {
                throw new IllegalStateException("RVD 批量结果包含未知策略：" + strategyId);
            }
            String rowId = row.getAs("row_id");
            String leftColumn = row.getAs("left_column");
            String rightColumn = row.getAs("right_column");
            long distinctRightCount = ((Number) row.getAs(
                    "distinct_right_count")).longValue();
            long groupSize = ((Number) row.getAs("group_size")).longValue();
            double score = SparkStrategySupport.boundedScore(
                    1.0d - 1.0d / distinctRightCount);
            String dependency = leftColumn + "->" + rightColumn;
            Map<String, String> leftDetails = SparkStrategySupport.details(
                    "dependency", dependency, "targetSide", "LEFT",
                    "distinctRightCount", String.valueOf(distinctRightCount),
                    "groupSize", String.valueOf(groupSize));
            Map<String, String> rightDetails = SparkStrategySupport.details(
                    "dependency", dependency, "targetSide", "RIGHT",
                    "distinctRightCount", String.valueOf(distinctRightCount),
                    "groupSize", String.valueOf(groupSize));
            hits.add(hit(jobId, stageId, dataset, strategyId, rowId,
                    leftColumn, row.getAs("left_hash"), leftDetails, score, runtimeMillis));
            hits.add(hit(jobId, stageId, dataset, strategyId, rowId,
                    rightColumn, row.getAs("right_hash"), rightDetails, score, runtimeMillis));
        }
        List<StrategyExecutionResult> results = new ArrayList<StrategyExecutionResult>(
                plans.size());
        for (StrategyPlan plan : plans) {
            List<StrategyHit> hits = hitsByStrategy.get(plan.getStrategyId());
            StrategyRunSummary summary = summary(jobId, stageId, dataset, plan,
                    StrategyStatus.SUCCEEDED, hits.size(), runtimeMillis, null, null);
            results.add(new StrategyExecutionResult(summary, hits));
        }
        return results;
    }

    private static StrategyHit hit(String jobId,
                                   String stageId,
                                   RahaDataset dataset,
                                   String strategyId,
                                   String rowId,
                                   String columnName,
                                   String valueHash,
                                   Map<String, String> details,
                                   double score,
                                   long runtimeMillis) {
        return new StrategyHit(jobId, stageId, strategyId, StrategyFamily.RVD,
                new CellCoordinate(dataset.getDatasetId(), dataset.getSnapshotId(),
                        rowId, columnName), valueHash, "RVD_ONE_TO_MANY_CONFLICT",
                details, score, runtimeMillis, StrategyStatus.SUCCEEDED);
    }

    private List<StrategyExecutionResult> failed(String jobId,
                                                  String stageId,
                                                  RahaDataset dataset,
                                                  List<StrategyPlan> plans,
                                                  long runtimeMillis,
                                                  String errorCode,
                                                  String errorMessage) {
        List<StrategyExecutionResult> results = new ArrayList<StrategyExecutionResult>(
                plans.size());
        for (StrategyPlan plan : plans) {
            results.add(new StrategyExecutionResult(summary(jobId, stageId, dataset, plan,
                    StrategyStatus.FAILED, 0L, runtimeMillis, errorCode, errorMessage),
                    Collections.<StrategyHit>emptyList()));
        }
        return results;
    }

    private StrategyRunSummary summary(String jobId,
                                       String stageId,
                                       RahaDataset dataset,
                                       StrategyPlan plan,
                                       StrategyStatus status,
                                       long hitCount,
                                       long runtimeMillis,
                                       String errorCode,
                                       String errorMessage) {
        ColumnProfile profile = dataset.getProfiles().get(plan.getTargetColumns().get(0));
        long inputCount = profile == null ? 0L : profile.getTotalCount();
        return new StrategyRunSummary(jobId, stageId, dataset.getSnapshotId(),
                plan.getStrategyId(), plan.getConfigurationHash(), StrategyFamily.RVD,
                status, inputCount, hitCount, runtimeMillis, errorCode, errorMessage,
                clock.millis());
    }

    private static void validate(String jobId,
                                 String stageId,
                                 RahaDataset dataset,
                                 List<StrategyPlan> plans,
                                 long timeoutMillis) {
        if (jobId == null || jobId.trim().isEmpty()
                || stageId == null || stageId.trim().isEmpty()
                || dataset == null || dataset.getDataFrame() == null
                || plans == null || timeoutMillis <= 0L) {
            throw new IllegalArgumentException("RVD 批量执行参数非法");
        }
        for (StrategyPlan plan : plans) {
            if (plan == null || plan.getStrategyFamily() != StrategyFamily.RVD
                    || !StrategyTypes.RVD_ONE_TO_MANY.equals(plan.getConfiguration().get(
                    StrategyConfigurationKeys.STRATEGY_TYPE))) {
                throw new IllegalArgumentException("RVD 批量执行只接受一对多关系策略");
            }
        }
    }

    private static void cancelSparkJob(RahaDataset dataset, String jobGroup) {
        try {
            dataset.getDataFrame().sparkSession().sparkContext().cancelJobGroup(jobGroup);
        } catch (RuntimeException exception) {
            LOGGER.warn("取消 RVD 批量 Spark 作业失败，jobGroup={}", jobGroup, exception);
        }
    }

    private static long elapsedMillis(long startNanos) {
        return Math.max(0L, TimeUnit.NANOSECONDS.toMillis(
                System.nanoTime() - startNanos));
    }

    private static String safeMessage(Throwable throwable) {
        String message = throwable == null ? null : throwable.getMessage();
        return message == null || message.trim().isEmpty()
                ? "RVD 批量执行失败" : message;
    }
}
