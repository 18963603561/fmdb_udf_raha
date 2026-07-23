package com.fiberhome.ml.raha.cluster;

import com.fiberhome.ml.raha.cluster.algorithm.ColumnClusterer;
import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringMetrics;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringStatus;
import com.fiberhome.ml.raha.config.dto.ClusteringConfig;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.parallel.ParallelBatchResult;
import com.fiberhome.ml.raha.parallel.ParallelWorkItem;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.ClusterRepository;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按字段隔离执行列内聚类，并保存版本化聚类摘要和成员映射。
 */
public final class ColumnClusteringService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnClusteringService.class);
    /** 可替换的单列聚类器。 */
    private final ColumnClusterer clusterer;
    /** 聚类结果仓储。 */
    private final ClusterRepository repository;
    /** 提供可测试更新时间的时钟。 */
    private final Clock clock;
    /** 受限列任务并行执行器。 */
    private final BoundedParallelExecutor parallelExecutor;

    public ColumnClusteringService(ColumnClusterer clusterer,
                                   ClusterRepository repository,
                                   Clock clock) {
        this(clusterer, repository, clock, new BoundedParallelExecutor());
    }

    public ColumnClusteringService(ColumnClusterer clusterer,
                                   ClusterRepository repository,
                                   Clock clock,
                                   BoundedParallelExecutor parallelExecutor) {
        if (clusterer == null || repository == null || clock == null) {
            throw new IllegalArgumentException("聚类服务依赖不能为空");
        }
        if (parallelExecutor == null) {
            throw new IllegalArgumentException("聚类并行执行器不能为空");
        }
        this.clusterer = clusterer;
        this.repository = repository;
        this.clock = clock;
        this.parallelExecutor = parallelExecutor;
    }

    /**
     * 按字段隔离串行执行聚类，并在同一业务版本下保存摘要和成员。
     *
     * @param jobId 任务标识
     * @param features 当前任务特征结果
     * @param config 聚类配置
     * @param randomSeed 可复现随机种子
     * @param version 仓储业务版本
     * @return 全字段聚类结果和指标
     */
    public ClusteringBatchResult clusterAndSave(String jobId,
                                                FeatureAssemblyResult features,
                                                ClusteringConfig config,
                                                long randomSeed,
                                                ArtifactVersion version) {
        if (features == null || config == null || version == null) {
            throw new IllegalArgumentException("聚类输入、配置和版本不能为空");
        }
        Map<String, List<SparseFeatureRow>> rowsByColumn = rowsByColumn(features.getRows());
        Map<String, ColumnClusteringResult> results =
                new LinkedHashMap<String, ColumnClusteringResult>();
        long clusteredColumnCount = 0L;
        long assignmentCount = 0L;
        long exceptionalColumnCount = 0L;
        LOGGER.info("开始列内聚类，jobId={}，columnCount={}，targetClusterCount={}，randomSeed={}",
                jobId, features.getDictionaries().size(), config.getTargetClusterCount(), randomSeed);
        for (Map.Entry<String, FeatureDictionary> entry
                : features.getDictionaries().entrySet()) {
            String columnName = entry.getKey();
            ColumnClusteringResult result;
            try {
                result = clusterer.cluster(columnName, entry.getValue(),
                        rowsByColumn.containsKey(columnName) ? rowsByColumn.get(columnName)
                                : java.util.Collections.<SparseFeatureRow>emptyList(),
                        config, randomSeed);
            } catch (RuntimeException exception) {
                // 单列聚类异常必须隔离，其他字段仍可继续生成采样所需聚类。
                LOGGER.error("列内聚类异常，jobId={}，columnName={}",
                        jobId, columnName, exception);
                result = failedResult(entry.getValue(), config, randomSeed, exception);
            }
            repository.save(jobId, result, version, clock.millis());
            results.put(columnName, result);
            assignmentCount += result.getAssignments().size();
            if (!result.getAssignments().isEmpty()) {
                clusteredColumnCount++;
            }
            if (result.getStatus() != ColumnClusteringStatus.SUCCEEDED
                    && result.getStatus() != ColumnClusteringStatus.SINGLE_SAMPLE) {
                exceptionalColumnCount++;
                LOGGER.warn("字段聚类返回非成功状态，jobId={}，columnName={}，status={}",
                        jobId, columnName, result.getStatus());
            }
        }
        ClusteringMetrics metrics = new ClusteringMetrics(results.size(), clusteredColumnCount,
                assignmentCount, exceptionalColumnCount);
        LOGGER.info("列内聚类完成，jobId={}，clusteredColumnCount={}，assignmentCount={}，exceptionalColumnCount={}",
                jobId, clusteredColumnCount, assignmentCount, exceptionalColumnCount);
        return new ClusteringBatchResult(results, metrics);
    }

    /**
     * 按字段受限并行执行聚类，单列算法异常仍转换为该列失败结果。
     *
     * <p>Spark MLlib 类聚类器会在 driver 侧提交 Spark 作业，不适合由本地线程池同时发起多列训练；
     * 这类聚类器会通过能力声明自动降级到串行入口。</p>
     *
     * @param jobId 任务标识
     * @param features 当前任务特征结果
     * @param config 聚类配置
     * @param randomSeed 可复现随机种子
     * @param version 仓储业务版本
     * @param maxParallelColumns 最大列并发数
     * @param timeoutMillis 聚类批次超时
     * @return 全字段聚类结果和指标
     */
    public ClusteringBatchResult clusterAndSaveParallel(
            String jobId,
            FeatureAssemblyResult features,
            ClusteringConfig config,
            long randomSeed,
            ArtifactVersion version,
            int maxParallelColumns,
            long timeoutMillis) {
        if (features == null || config == null || version == null
                || maxParallelColumns <= 0 || timeoutMillis <= 0L) {
            throw new IllegalArgumentException("并行聚类输入、并发数和超时必须有效");
        }
        if (!clusterer.supportsLocalColumnParallelism()) {
            // Spark MLlib 聚类由 driver 提交 Spark 作业，避免本地线程池同时发起多列训练。
            LOGGER.warn("当前聚类器不支持本地列并发，降级为串行列聚类，jobId={}，algorithm={}，requestedMaxParallelColumns={}",
                    jobId, clusterer.getAlgorithm(), maxParallelColumns);
            return clusterAndSave(jobId, features, config, randomSeed, version);
        }
        Map<String, List<SparseFeatureRow>> rowsByColumn = rowsByColumn(features.getRows());
        List<ParallelWorkItem<String, ColumnClusteringResult>> items =
                new ArrayList<ParallelWorkItem<String, ColumnClusteringResult>>();
        for (Map.Entry<String, FeatureDictionary> entry
                : features.getDictionaries().entrySet()) {
            String columnName = entry.getKey();
            List<SparseFeatureRow> rows = rowsByColumn.containsKey(columnName)
                    ? rowsByColumn.get(columnName)
                    : java.util.Collections.<SparseFeatureRow>emptyList();
            items.add(new ParallelWorkItem<String, ColumnClusteringResult>(
                    columnName, () -> clusterColumn(jobId, entry.getValue(), rows,
                    config, randomSeed)));
        }
        LOGGER.info("开始并行列内聚类，jobId={}，columnCount={}，maxParallelColumns={}",
                jobId, items.size(), maxParallelColumns);
        ParallelBatchResult<String, ColumnClusteringResult> parallel =
                parallelExecutor.execute(items, maxParallelColumns, timeoutMillis);
        if (!parallel.getFailures().isEmpty()) {
            throw new IllegalStateException("列聚类并行调度失败："
                    + parallel.getFailures().keySet());
        }
        Map<String, ColumnClusteringResult> results =
                new LinkedHashMap<String, ColumnClusteringResult>();
        long clusteredColumnCount = 0L;
        long assignmentCount = 0L;
        long exceptionalColumnCount = 0L;
        for (String columnName : features.getDictionaries().keySet()) {
            ColumnClusteringResult result = parallel.getSuccesses().get(columnName);
            repository.save(jobId, result, version, clock.millis());
            results.put(columnName, result);
            assignmentCount += result.getAssignments().size();
            if (!result.getAssignments().isEmpty()) {
                clusteredColumnCount++;
            }
            if (result.getStatus() != ColumnClusteringStatus.SUCCEEDED
                    && result.getStatus() != ColumnClusteringStatus.SINGLE_SAMPLE) {
                exceptionalColumnCount++;
            }
        }
        LOGGER.info("并行列内聚类完成，jobId={}，assignmentCount={}，maxObservedConcurrency={}",
                jobId, assignmentCount, parallel.getMaxObservedConcurrency());
        return new ClusteringBatchResult(results, new ClusteringMetrics(
                results.size(), clusteredColumnCount, assignmentCount,
                exceptionalColumnCount));
    }

    /**
     * 释放已经保存到外部检查点的任务级列聚类缓存。
     */
    public void releaseCachedBatch(String jobId, Set<String> columns) {
        repository.release(jobId, columns);
    }

    private ColumnClusteringResult clusterColumn(String jobId,
                                                 FeatureDictionary dictionary,
                                                 List<SparseFeatureRow> rows,
                                                 ClusteringConfig config,
                                                 long randomSeed) {
        try {
            return clusterer.cluster(dictionary.getColumnName(), dictionary,
                    rows, config, randomSeed);
        } catch (RuntimeException exception) {
            // 并行模式下单列异常同样隔离，防止一个字段取消其他已运行字段。
            LOGGER.error("并行列内聚类异常，jobId={}，columnName={}",
                    jobId, dictionary.getColumnName(), exception);
            return failedResult(dictionary, config, randomSeed, exception);
        }
    }

    private static Map<String, List<SparseFeatureRow>> rowsByColumn(
            List<SparseFeatureRow> rows) {
        Map<String, List<SparseFeatureRow>> index =
                new LinkedHashMap<String, List<SparseFeatureRow>>();
        for (SparseFeatureRow row : rows) {
            if (!index.containsKey(row.getColumnName())) {
                index.put(row.getColumnName(), new ArrayList<SparseFeatureRow>());
            }
            index.get(row.getColumnName()).add(row);
        }
        return index;
    }

    private ColumnClusteringResult failedResult(FeatureDictionary dictionary,
                                                ClusteringConfig config,
                                                long randomSeed,
                                                RuntimeException exception) {
        ClusterVersioner localVersioner = new ClusterVersioner();
        String version = localVersioner.versionOf(dictionary.getColumnName(),
                dictionary.getVersion(), clusterer.getAlgorithm(), config, randomSeed,
                ColumnClusteringStatus.FAILED,
                java.util.Collections.<String, String>emptyMap());
        return new ColumnClusteringResult(dictionary.getColumnName(),
                clusterer.getAlgorithm(), config.getDistanceMetric(),
                config.getTargetClusterCount(), 0, randomSeed, version,
                ColumnClusteringStatus.FAILED,
                "聚类异常已隔离：" + exception.getClass().getSimpleName(),
                java.util.Collections.<ClusterAssignment>emptyList(), clock.millis());
    }
}
