package com.fiberhome.ml.raha.repository.adapter.fmdb.result;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceVerifier;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPartitionUtils;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.service.SampleMaterializationResult;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.service.detect.RahaDetectOutput;
import com.fiberhome.ml.raha.service.train.RahaTrainOutput;
import com.fiberhome.ml.raha.service.train.TrainingArtifactMaterializationResult;
import com.fiberhome.ml.raha.service.train.TrainingMergeResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 完成检测错误表写入，并对采样、训练和检测物理表执行数量及批次回读校验。
 */
public final class FmdbResultPersistenceVerifier
        implements ResultPersistenceVerifier {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbResultPersistenceVerifier.class);
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;

    public FmdbResultPersistenceVerifier(FmdbTableGateway tableGateway,
                                         FmdbPersistenceConfig persistenceConfig) {
        if (tableGateway == null || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 结果验证器依赖不能为空");
        }
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
    }

    @Override
    public String verify(StageExecutionContext context,
                         RahaServiceResult<?> result) {
        if (context == null || result == null
                || !context.getJob().getJobId().equals(result.getJobId())) {
            throw new IllegalArgumentException("结果验证任务上下文不一致");
        }
        LOGGER.info("开始验证业务物理结果，jobId={}，jobType={}",
                result.getJobId(), result.getJobType());
        String location;
        if (result.getJobType() == JobType.SAMPLING) {
            location = verifySample(context);
        } else if (result.getJobType() == JobType.TRAINING) {
            location = verifyTraining(context, result);
        } else if (result.getJobType() == JobType.DETECTION) {
            location = persistAndVerifyDetection(context, result);
        } else {
            throw new IllegalArgumentException("未支持的物理结果类型："
                    + result.getJobType());
        }
        LOGGER.info("业务物理结果验证完成，jobId={}，jobType={}，location={}",
                result.getJobId(), result.getJobType(), location);
        return location;
    }

    private String verifySample(StageExecutionContext context) {
        Object value = context.getAttributes().get(
                StageAttributeKeys.SAMPLE_MATERIALIZATION_RESULT);
        if (!(value instanceof SampleMaterializationResult)) {
            throw new IllegalStateException("采样结果缺少物理写入回执");
        }
        SampleMaterializationResult receipt = (SampleMaterializationResult) value;
        SampleBatch batch = receipt.getBatch();
        if (!receipt.isPersisted()) {
            throw new IllegalStateException("采样批次未完成物理写入");
        }
        long actual = count(FmdbPhysicalTable.SAMPLE_RECORD,
                functions.col("dataset_id").equalTo(batch.getDatasetId())
                        .and(functions.col("partition_month")
                                .equalTo(batch.getPartitionMonth()))
                        .and(functions.col("sample_batch_id")
                                .equalTo(batch.getSampleBatchId())), "row_id");
        requireCount("采样记录", batch.getRecords().size(), actual);
        return receipt.getResultLocation();
    }

    private String verifyTraining(StageExecutionContext context,
                                  RahaServiceResult<?> result) {
        if (!(result.getPayload() instanceof RahaTrainOutput)) {
            throw new IllegalStateException("训练结果缺少类型化输出");
        }
        RahaTrainOutput output = (RahaTrainOutput) result.getPayload();
        TrainingArtifactMaterializationResult materialization =
                output.getMaterializationResult();
        Object mergeValue = context.getAttributes().get(
                StageAttributeKeys.TRAINING_MERGE_RESULT);
        if (materialization == null || !(mergeValue instanceof TrainingMergeResult)
                || output.getModelSetVersion() == null) {
            throw new IllegalStateException("训练结果缺少合并和物理写入回执");
        }
        TrainingMergeResult merge = (TrainingMergeResult) mergeValue;
        String datasetId = merge.getDataset().getDatasetId();
        long artifacts = count(FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT,
                functions.col("dataset_id").equalTo(datasetId)
                        .and(functions.col("training_batch_id")
                                .equalTo(merge.getTrainingBatchId())), "column_name");
        requireCount("训练列级产物", output.getFeatures().getDictionaries().size(),
                artifacts);
        long examples = count(FmdbPhysicalTable.TRAINING_EXAMPLE,
                functions.col("dataset_id").equalTo(datasetId)
                        .and(functions.col("partition_month").equalTo(
                                materialization.getPartitionMonth()))
                        .and(functions.col("model_set_version").equalTo(
                                output.getModelSetVersion())), "cell_id");
        requireCount("最终训练样本",
                materialization.getMaterializedExampleCount(), examples);
        long models = distinctCount(FmdbPhysicalTable.MODEL_ARTIFACT,
                functions.col("dataset_id").equalTo(datasetId)
                        .and(functions.col("model_set_version")
                                .equalTo(output.getModelSetVersion())), "model_version");
        requireCount("候选模型", output.getCandidateModels().size(), models);
        return "fmdb://" + FmdbPhysicalTable.MODEL_ARTIFACT.getTableName()
                + "/" + output.getModelSetVersion();
    }

    private String persistAndVerifyDetection(StageExecutionContext context,
                                             RahaServiceResult<?> result) {
        if (!(result.getPayload() instanceof RahaDetectOutput)
                || !persistenceConfig.shouldPersist(
                FmdbPhysicalTable.DETECTION_RESULT)) {
            throw new IllegalStateException("检测错误结果未开启物理持久化或输出缺失");
        }
        Object datasetValue = context.getAttributes().get(StageAttributeKeys.RAHA_DATASET);
        if (!(datasetValue instanceof RahaDataset)) {
            throw new IllegalStateException("检测结果缺少可信输入数据集");
        }
        RahaDetectOutput output = (RahaDetectOutput) result.getPayload();
        RahaDataset dataset = (RahaDataset) datasetValue;
        List<DetectionResult> errors = new ArrayList<DetectionResult>();
        Set<String> partitionDates = new LinkedHashSet<String>();
        for (DetectionResult detection : output.getResults()) {
            if (detection.isError()) {
                errors.add(detection);
                partitionDates.add(FmdbPartitionUtils.date(detection.getDetectedAt()));
            }
        }
        long actual = 0L;
        if (!errors.isEmpty()) {
            Column condition = functions.col("dataset_id")
                    .equalTo(dataset.getDatasetId())
                    .and(functions.col("detection_batch_id")
                            .equalTo(result.getJobId()))
                    .and(functions.col("partition_date").isin(
                            partitionDates.toArray(new Object[0])));
            actual = count(FmdbPhysicalTable.DETECTION_RESULT, condition, "cell_id");
        }
        requireCount("检测错误结果", errors.size(), actual);
        return "fmdb://" + FmdbPhysicalTable.DETECTION_RESULT.getTableName()
                + "/" + result.getJobId();
    }

    private long count(FmdbPhysicalTable table, Column condition, String projection) {
        if (!tableGateway.tableExists(table.getTableName())) {
            return 0L;
        }
        return tableGateway.read(table.getTableName(),
                Collections.singletonList(projection), condition).count();
    }

    private long distinctCount(FmdbPhysicalTable table, Column condition,
                               String projection) {
        if (!tableGateway.tableExists(table.getTableName())) {
            return 0L;
        }
        return tableGateway.read(table.getTableName(),
                Collections.singletonList(projection), condition).distinct().count();
    }

    private static void requireCount(String name, long expected, long actual) {
        if (expected != actual) {
            throw new IllegalStateException(name + "物理数量不一致，expected="
                    + expected + "，actual=" + actual);
        }
    }
}
