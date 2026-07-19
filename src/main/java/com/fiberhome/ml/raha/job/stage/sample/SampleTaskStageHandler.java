package com.fiberhome.ml.raha.job.stage.sample;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.job.stage.core.ServiceStageResultMapper;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleRequest;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
import com.fiberhome.ml.raha.sampling.service.SampleMaterializationResult;
import com.fiberhome.ml.raha.sampling.service.SampleRecordService;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 复用前序特征和聚类结果执行任务级采样服务。
 */
public final class SampleTaskStageHandler implements StageHandler {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SampleTaskStageHandler.class);
    /** 任务级采样服务。 */
    private final RahaSampleService sampleService;
    /** 可信 c1 行回取和物理持久化服务。 */
    private final SampleRecordService sampleRecordService;
    /** 当前采样轮次。 */
    private final int samplingRound;
    /** 工作流调用方已经持有的标签。 */
    private final List<CellLabel> initialLabels;

    public SampleTaskStageHandler(RahaSampleService sampleService,
                                  SampleRecordService sampleRecordService,
                                  int samplingRound,
                                  List<CellLabel> initialLabels) {
        if (sampleService == null || sampleRecordService == null
                || samplingRound <= 0) {
            throw new IllegalArgumentException("任务级采样和持久化服务不能为空且轮次必须大于零");
        }
        if (initialLabels == null) {
            throw new IllegalArgumentException("采样初始标签不能为空");
        }
        this.sampleService = sampleService;
        this.sampleRecordService = sampleRecordService;
        this.samplingRound = samplingRound;
        this.initialLabels = Collections.unmodifiableList(
                new ArrayList<CellLabel>(initialLabels));
    }

    @Override
    public StageType getStageType() {
        return StageType.SAMPLE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public StageResult execute(StageExecutionContext context) {
        Object featureValue = context.getAttributes().get(
                StageAttributeKeys.FEATURE_ASSEMBLY_RESULT);
        Object clusteringValue = context.getAttributes().get(
                StageAttributeKeys.CLUSTERING_BATCH_RESULT);
        if (!(featureValue instanceof FeatureAssemblyResult)
                || !(clusteringValue instanceof ClusteringBatchResult)) {
            return StageResult.failure("SAMPLING_INPUT_REQUIRED",
                    "任务采样阶段缺少特征或聚类结果", false, 0L, 0L);
        }
        Object labelValue = context.getAttributes().get(StageAttributeKeys.CELL_LABELS);
        List<CellLabel> labels = labelValue instanceof List
                ? (List<CellLabel>) labelValue : initialLabels;
        ArtifactVersion version = new ArtifactVersion(
                context.getJob().getConfigVersion(), context.getJob().getSnapshotId(),
                context.getStage().getStageId(), context.getStage().getAttemptId());
        RahaServiceResult<RahaSampleOutput> result = sampleService.sample(
                new RahaSampleRequest(context.getJob().getJobId(),
                        (FeatureAssemblyResult) featureValue, labels,
                        context.getConfig().getClusteringConfig(),
                        context.getConfig().getSamplingConfig(), samplingRound,
                        context.getConfig().getRandomSeed(), version,
                        context.getConfig().getResourceConfig(),
                        (ClusteringBatchResult) clusteringValue));
        if (result.getStatus() == JobStatus.SUCCEEDED
                && result.getPayload() != null) {
            result = persistSampleBatch(context, result);
        }
        context.getAttributes().put(StageAttributeKeys.SAMPLE_SERVICE_RESULT, result);
        if (result.getPayload() != null) {
            context.getAttributes().put(StageAttributeKeys.SAMPLE_OUTPUT, result.getPayload());
            context.getAttributes().put(StageAttributeKeys.SAMPLING_BATCH_RESULT,
                    result.getPayload().getSampling());
            context.getAttributes().put(StageAttributeKeys.ANNOTATION_TASKS,
                    result.getPayload().getSampling().getTasks());
        }
        return ServiceStageResultMapper.map(result);
    }

    private RahaServiceResult<RahaSampleOutput> persistSampleBatch(
            StageExecutionContext context,
            RahaServiceResult<RahaSampleOutput> result) {
        Object datasetValue = context.getAttributes().get(
                StageAttributeKeys.RAHA_DATASET);
        Object snapshotValue = context.getAttributes().get(
                StageAttributeKeys.DATASET_SNAPSHOT);
        if (!(datasetValue instanceof RahaDataset)
                || !(snapshotValue instanceof DatasetSnapshot)) {
            return failedPersistence(result, "SAMPLE_DATASET_REQUIRED",
                    "c1 持久化缺少可信数据集或快照");
        }
        try {
            SampleMaterializationResult materialized =
                    sampleRecordService.materializeAndPersist(
                            (RahaDataset) datasetValue,
                            (DatasetSnapshot) snapshotValue,
                            context.getConfig().getRowIdentityConfig(),
                            result.getPayload().getSampling());
            // 首个可用闭环要求采样成功时 c1 已真实入库，关闭表开关不能伪造成功。
            if (!materialized.isPersisted()) {
                return failedPersistence(result,
                        "SAMPLE_PERSISTENCE_DISABLED",
                        "c1 采样物理表入库已关闭");
            }
            RahaSampleOutput output = result.getPayload().withSampleBatch(
                    materialized.getBatch());
            context.getAttributes().put(StageAttributeKeys.SAMPLE_BATCH,
                    materialized.getBatch());
            context.getAttributes().put(
                    StageAttributeKeys.SAMPLE_MATERIALIZATION_RESULT, materialized);
            return new RahaServiceResult<RahaSampleOutput>(result.getJobId(),
                    result.getJobType(), result.getStatus(),
                    materialized.getResultLocation(), result.getSummary(), output,
                    null, null);
        } catch (RuntimeException exception) {
            LOGGER.error("c1 采样批次持久化失败，jobId={}，samplingRound={}",
                    context.getJob().getJobId(), samplingRound, exception);
            return failedPersistence(result, "SAMPLE_PERSISTENCE_FAILED",
                    exception.getClass().getSimpleName());
        }
    }

    private static RahaServiceResult<RahaSampleOutput> failedPersistence(
            RahaServiceResult<RahaSampleOutput> result,
            String errorCode,
            String errorMessage) {
        return new RahaServiceResult<RahaSampleOutput>(result.getJobId(),
                result.getJobType(), JobStatus.FAILED, null,
                result.getSummary(), null, errorCode, errorMessage);
    }
}
