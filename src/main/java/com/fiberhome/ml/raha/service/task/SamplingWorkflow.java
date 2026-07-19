package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.stage.feature.ClusterStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceVerifier;
import com.fiberhome.ml.raha.job.stage.sample.SampleTaskStageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.service.sample.RahaSampleService;
import com.fiberhome.ml.raha.sampling.service.SampleRecordService;
import com.fiberhome.ml.raha.strategy.execution.StrategyExecutionService;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlanService;
import java.util.Collections;
import java.util.List;

/**
 * 创建从数据加载到标注任务持久化的采样工作流。
 */
public final class SamplingWorkflow extends AbstractRahaWorkflow {

    /** 列内聚类服务。 */
    private final ColumnClusteringService clusteringService;
    /** 任务级采样服务。 */
    private final RahaSampleService sampleService;
    /** c1 采样宽表物化服务。 */
    private final SampleRecordService sampleRecordService;
    /** 可选采样物理结果回读验证器。 */
    private final ResultPersistenceVerifier resultPersistenceVerifier;

    public SamplingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            RahaSampleService sampleService,
                            SampleRecordService sampleRecordService) {
        this(datasetLoader, profileService, planService, executionService,
                featureService, clusteringService, sampleService,
                sampleRecordService, null);
    }

    public SamplingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            RahaSampleService sampleService,
                            SampleRecordService sampleRecordService,
                            ResultPersistenceVerifier resultPersistenceVerifier) {
        super(datasetLoader, profileService, planService, executionService, featureService);
        if (clusteringService == null || sampleService == null
                || sampleRecordService == null) {
            throw new IllegalArgumentException("采样工作流业务依赖不能为空");
        }
        this.clusteringService = clusteringService;
        this.sampleService = sampleService;
        this.sampleRecordService = sampleRecordService;
        this.resultPersistenceVerifier = resultPersistenceVerifier;
    }

    @Override
    public JobType getJobType() {
        return JobType.SAMPLING;
    }

    @Override
    public List<StageHandler> createStageHandlers(RahaTaskExecutionRequest request) {
        if (request == null || request.getConfig().getJobType() != JobType.SAMPLING) {
            throw new IllegalArgumentException("采样工作流请求类型不匹配");
        }
        List<StageHandler> handlers = preparationStages(request);
        handlers.add(new ClusterStageHandler(clusteringService));
        handlers.add(new SampleTaskStageHandler(sampleService, sampleRecordService,
                request.getSamplingRound(), request.getLabels()));
        handlers.add(new ResultPersistenceStageHandler(
                StageAttributeKeys.SAMPLE_SERVICE_RESULT,
                resultPersistenceVerifier));
        return Collections.unmodifiableList(handlers);
    }
}
