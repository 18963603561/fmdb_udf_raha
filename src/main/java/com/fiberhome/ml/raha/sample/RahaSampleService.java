package com.fiberhome.ml.raha.sample;

import com.fiberhome.ml.raha.api.SampleRequest;
import com.fiberhome.ml.raha.api.SampleResult;
import com.fiberhome.ml.raha.config.RahaConfig;
import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.profile.ColumnProfiler;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyPlanner;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;
import com.fiberhome.ml.raha.support.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 同步采样用例，完成输入解析、策略画像、元组选择和批次提交。
 */
public final class RahaSampleService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaSampleService.class);
    /** FMDB 数据加载器。 */
    private final FmdbDatasetLoader datasetLoader;
    /** 列画像组件。 */
    private final ColumnProfiler columnProfiler;
    /** 策略计划生成器。 */
    private final StrategyPlanner strategyPlanner;
    /** 元组采样器。 */
    private final TupleSampler tupleSampler;
    /** 采样存储端口。 */
    private final SampleStore sampleStore;
    /** 根配置。 */
    private final RahaConfig config;

    public RahaSampleService(FmdbDatasetLoader datasetLoader,
                             ColumnProfiler columnProfiler,
                             StrategyPlanner strategyPlanner,
                             TupleSampler tupleSampler,
                             SampleStore sampleStore,
                             RahaConfig config) {
        this.datasetLoader = datasetLoader;
        this.columnProfiler = columnProfiler;
        this.strategyPlanner = strategyPlanner;
        this.tupleSampler = tupleSampler;
        this.sampleStore = sampleStore;
        this.config = config;
    }

    public SampleResult sample(SampleRequest request) {
        long startedAt = System.currentTimeMillis();
        RahaDataset dataset = datasetLoader.load(request);
        try {
            int budget = request.getLabelingBudget() <= 0
                    ? config.getDefaultLabelingBudget() : request.getLabelingBudget();
            if (budget <= 0 || budget > config.getMaximumLabelingBudget()) {
                throw new RahaException(RahaErrorCode.INVALID_REQUEST,
                        "标注预算超出范围：" + budget);
            }
            String requestFingerprint = fingerprint(dataset, budget);
            String batchId = HashUtils.shortId("sample", requestFingerprint);
            Optional<SampleBatch> existing = sampleStore.findBatch(batchId);
            if (existing.isPresent()) {
                SampleBatch batch = existing.get();
                LOGGER.info("采样请求命中既有批次，sampleBatchId={}，targetColumns={}",
                        batchId, batch.getTargetColumns());
                return new SampleResult(batchId, batch.getTargetColumns(),
                        batch.getSelectedTupleCount(), "fmdb://dw.raha_sample_tuple/"
                        + batchId, System.currentTimeMillis() - startedAt);
            }
            List<ColumnProfile> profiles = columnProfiler.profile(dataset);
            StrategyPlan plan = strategyPlanner.plan(profiles);
            long createdAt = System.currentTimeMillis();
            String partitionDate = TimeUtils.partitionDate(createdAt,
                    config.getPartitionTimeZone());
            List<SampleTuple> tuples = tupleSampler.sample(dataset, budget, batchId,
                    createdAt, partitionDate);
            Map<String, Object> configValues = new LinkedHashMap<String, Object>();
            configValues.put("strategyPlanVersion", plan.getVersion());
            configValues.put("randomSeed", config.getRandomSeed());
            configValues.put("labelingBudget", budget);
            SampleBatch batch = new SampleBatch(batchId, requestFingerprint,
                    dataset.getDatasetId(), dataset.getSnapshotId(),
                    dataset.getInputReference(), dataset.getSourceType(),
                    dataset.getRowIdentityMode(), dataset.getRowKeyColumns(),
                    dataset.getTargetColumns(), dataset.getSchemaHash(),
                    config.getAlgorithmVersion(), JsonUtils.toJson(configValues), budget,
                    tuples.size(), createdAt);
            sampleStore.save(batch, tuples);
            LOGGER.info("采样完成，sampleBatchId={}，datasetId={}，targetColumns={}，"
                            + "inputRows={}，selectedTuples={}，elapsedMillis={}",
                    batchId, dataset.getDatasetId(), dataset.getTargetColumns(),
                    dataset.getInputRowCount(), tuples.size(),
                    System.currentTimeMillis() - startedAt);
            return new SampleResult(batchId, dataset.getTargetColumns(), tuples.size(),
                    "fmdb://dw.raha_sample_tuple/" + batchId,
                    System.currentTimeMillis() - startedAt);
        } catch (RuntimeException exception) {
            LOGGER.error("采样失败，datasetId={}，snapshotId={}，targetColumns={}",
                    dataset.getDatasetId(), dataset.getSnapshotId(),
                    dataset.getTargetColumns(), exception);
            throw exception;
        } finally {
            dataset.getRows().unpersist(false);
        }
    }

    private String fingerprint(RahaDataset dataset, int budget) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("datasetId", dataset.getDatasetId());
        values.put("snapshotId", dataset.getSnapshotId());
        values.put("inputReference", dataset.getInputReference());
        values.put("sourceType", dataset.getSourceType());
        values.put("rowIdentityMode", dataset.getRowIdentityMode().name());
        values.put("rowKeyColumns", dataset.getRowKeyColumns());
        values.put("targetColumns", dataset.getTargetColumns());
        values.put("schemaHash", dataset.getSchemaHash());
        values.put("algorithmVersion", config.getAlgorithmVersion());
        values.put("labelingBudget", budget);
        values.put("randomSeed", config.getRandomSeed());
        return HashUtils.sha256(JsonUtils.toJson(values));
    }
}
