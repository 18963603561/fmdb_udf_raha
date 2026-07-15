package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.config.FeatureConfig;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.parallel.BoundedParallelExecutor;
import com.fiberhome.ml.raha.parallel.ParallelBatchResult;
import com.fiberhome.ml.raha.parallel.ParallelWorkItem;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.FeatureRepository;
import com.fiberhome.ml.raha.strategy.StrategyHit;
import com.fiberhome.ml.raha.strategy.StrategyPlan;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 组装并版本化保存单元格特征。
 */
public final class FeatureService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureService.class);
    /** 特征组装器。 */
    private final FeatureAssembler assembler;
    /** 特征仓储。 */
    private final FeatureRepository repository;
    /** 提供可测试更新时间的时钟。 */
    private final Clock clock;
    /** 受限列任务并行执行器。 */
    private final BoundedParallelExecutor parallelExecutor;

    public FeatureService(FeatureAssembler assembler,
                          FeatureRepository repository,
                          Clock clock) {
        this(assembler, repository, clock, new BoundedParallelExecutor());
    }

    public FeatureService(FeatureAssembler assembler,
                          FeatureRepository repository,
                          Clock clock,
                          BoundedParallelExecutor parallelExecutor) {
        if (assembler == null || repository == null || clock == null) {
            throw new IllegalArgumentException("特征服务依赖不能为空");
        }
        if (parallelExecutor == null) {
            throw new IllegalArgumentException("特征并行执行器不能为空");
        }
        this.assembler = assembler;
        this.repository = repository;
        this.clock = clock;
        this.parallelExecutor = parallelExecutor;
    }

    public FeatureAssemblyResult assembleAndSave(String jobId,
                                                 RahaDataset dataset,
                                                 List<StrategyPlan> plans,
                                                 List<StrategyHit> hits,
                                                 FeatureConfig config,
                                                 ArtifactVersion version) {
        FeatureAssemblyResult result = assembler.assemble(dataset, plans, hits, config);
        repository.save(jobId, result, version, clock.millis());
        return result;
    }

    /**
     * 将可检测字段拆成独立列任务并受限并行组装，最后合并为一个版本化特征结果。
     *
     * @param maxParallelColumns 最大列并发数
     * @param timeoutMillis 特征批次超时
     * @return 按数据集字段顺序合并的特征结果
     */
    public FeatureAssemblyResult assembleAndSaveParallel(
            String jobId,
            RahaDataset dataset,
            List<StrategyPlan> plans,
            List<StrategyHit> hits,
            FeatureConfig config,
            ArtifactVersion version,
            int maxParallelColumns,
            long timeoutMillis) {
        if (dataset == null || plans == null || hits == null || config == null
                || version == null || maxParallelColumns <= 0 || timeoutMillis <= 0L) {
            throw new IllegalArgumentException("并行特征输入、并发数和超时必须有效");
        }
        List<ParallelWorkItem<String, FeatureAssemblyResult>> items =
                new ArrayList<ParallelWorkItem<String, FeatureAssemblyResult>>();
        for (ColumnMetadata column : dataset.getColumns()) {
            if (!column.isDetectable()) {
                continue;
            }
            items.add(new ParallelWorkItem<String, FeatureAssemblyResult>(
                    column.getName(), () -> assembler.assemble(
                    isolate(dataset, column), plansForColumn(plans, column.getName()),
                    hitsForColumn(hits, column.getName()), config)));
        }
        LOGGER.info("开始并行组装列特征，jobId={}，columnCount={}，maxParallelColumns={}",
                jobId, items.size(), maxParallelColumns);
        ParallelBatchResult<String, FeatureAssemblyResult> parallel =
                parallelExecutor.execute(items, maxParallelColumns, timeoutMillis);
        if (!parallel.getFailures().isEmpty()) {
            throw new IllegalStateException("列特征并行执行失败："
                    + parallel.getFailures().keySet());
        }
        FeatureAssemblyResult merged = merge(parallel.getSuccesses());
        repository.save(jobId, merged, version, clock.millis());
        LOGGER.info("并行列特征组装完成，jobId={}，rowCount={}，"
                        + "maxObservedConcurrency={}",
                jobId, merged.getRows().size(), parallel.getMaxObservedConcurrency());
        return merged;
    }

    private static RahaDataset isolate(RahaDataset dataset, ColumnMetadata target) {
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        Map<String, ColumnProfile> profiles = new LinkedHashMap<String, ColumnProfile>();
        for (ColumnMetadata column : dataset.getColumns()) {
            if (column.getName().equals(dataset.getRowIdColumn())
                    || column.getName().equals(target.getName())) {
                columns.add(column);
                if (dataset.getProfiles().containsKey(column.getName())) {
                    profiles.put(column.getName(), dataset.getProfiles().get(column.getName()));
                }
            }
        }
        return new RahaDataset(dataset.getDatasetId(), dataset.getSnapshotId(),
                dataset.getTableName(), dataset.getRowIdColumn(), columns,
                dataset.getDataFrame(), dataset.getSchemaHash(), profiles);
    }

    private static List<StrategyPlan> plansForColumn(List<StrategyPlan> plans,
                                                      String columnName) {
        List<StrategyPlan> matches = new ArrayList<StrategyPlan>();
        for (StrategyPlan plan : plans) {
            if (plan.getTargetColumns().contains(columnName)) {
                matches.add(plan);
            }
        }
        return matches;
    }

    private static List<StrategyHit> hitsForColumn(List<StrategyHit> hits,
                                                    String columnName) {
        List<StrategyHit> matches = new ArrayList<StrategyHit>();
        for (StrategyHit hit : hits) {
            if (hit.getCoordinate().getColumnName().equals(columnName)) {
                matches.add(hit);
            }
        }
        return matches;
    }

    private static FeatureAssemblyResult merge(
            Map<String, FeatureAssemblyResult> columnResults) {
        Map<String, FeatureDictionary> dictionaries =
                new LinkedHashMap<String, FeatureDictionary>();
        List<SparseFeatureRow> rows = new ArrayList<SparseFeatureRow>();
        long cellCount = 0L;
        long candidateCount = 0L;
        long retainedCount = 0L;
        long removedCount = 0L;
        for (FeatureAssemblyResult result : columnResults.values()) {
            dictionaries.putAll(result.getDictionaries());
            rows.addAll(result.getRows());
            cellCount += result.getMetrics().getCellCount();
            candidateCount += result.getMetrics().getCandidateFeatureCount();
            retainedCount += result.getMetrics().getRetainedFeatureCount();
            removedCount += result.getMetrics().getRemovedConstantFeatureCount();
        }
        return new FeatureAssemblyResult(dictionaries, rows,
                new FeatureAssemblyMetrics(cellCount, candidateCount,
                        retainedCount, removedCount));
    }
}
