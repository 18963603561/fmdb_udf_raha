package com.fiberhome.ml.raha.job.stage.sample;

import com.fiberhome.ml.raha.job.stage.core.ServiceStageResultMapper;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageExecutionContext;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageResult;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.service.sample.RahaSampleOutput;
import com.fiberhome.ml.raha.service.sample.RahaSampleRequest;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

/**
 * 复用前序特征和聚类结果执行任务级采样服务。
 */
public final class SampleTaskStageHandler implements StageHandler {

    /** 任务级采样服务。 */
    private final RahaSampleService sampleService;
    /** 当前采样轮次。 */
    private final int samplingRound;
    /** 工作流调用方已经持有的标签。 */
    private final List<CellLabel> initialLabels;

    public SampleTaskStageHandler(RahaSampleService sampleService, int samplingRound) {
        this(sampleService, samplingRound, Collections.<CellLabel>emptyList());
    }

    public SampleTaskStageHandler(RahaSampleService sampleService,
                                  int samplingRound,
                                  List<CellLabel> initialLabels) {
        if (sampleService == null || samplingRound <= 0) {
            throw new IllegalArgumentException("任务级采样服务不能为空且轮次必须大于零");
        }
        if (initialLabels == null) {
            throw new IllegalArgumentException("采样初始标签不能为空");
        }
        this.sampleService = sampleService;
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
}
