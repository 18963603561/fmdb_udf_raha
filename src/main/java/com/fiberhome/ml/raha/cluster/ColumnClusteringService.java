package com.fiberhome.ml.raha.cluster;

import com.fiberhome.ml.raha.config.ClusteringConfig;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.ClusterRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按字段隔离执行列内聚类并保存版本化结果。
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

    public ColumnClusteringService(ColumnClusterer clusterer,
                                   ClusterRepository repository,
                                   Clock clock) {
        if (clusterer == null || repository == null || clock == null) {
            throw new IllegalArgumentException("聚类服务依赖不能为空");
        }
        this.clusterer = clusterer;
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 按字段隔离执行聚类，并在同一业务版本下保存摘要和成员。
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
        LOGGER.info("列内聚类完成，jobId={}，clusteredColumnCount={}，assignmentCount={}，"
                        + "exceptionalColumnCount={}",
                jobId, clusteredColumnCount, assignmentCount, exceptionalColumnCount);
        return new ClusteringBatchResult(results, metrics);
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
        ClusterVersioner versioner = new ClusterVersioner();
        String version = versioner.versionOf(dictionary.getColumnName(), dictionary.getVersion(),
                clusterer.getAlgorithm(), config, randomSeed,
                ColumnClusteringStatus.FAILED, java.util.Collections.<String, String>emptyMap());
        return new ColumnClusteringResult(dictionary.getColumnName(),
                clusterer.getAlgorithm(), config.getDistanceMetric(),
                config.getTargetClusterCount(), 0, randomSeed, version,
                ColumnClusteringStatus.FAILED,
                "聚类异常已隔离：" + exception.getClass().getSimpleName(),
                java.util.Collections.<ClusterAssignment>emptyList(), clock.millis());
    }
}
