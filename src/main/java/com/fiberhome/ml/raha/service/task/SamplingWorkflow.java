package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.data.loader.RahaDatasetLoader;
import com.fiberhome.ml.raha.data.profile.ColumnProfileService;
import com.fiberhome.ml.raha.data.type.JobType;
import com.fiberhome.ml.raha.feature.FeatureService;
import com.fiberhome.ml.raha.job.stage.checkpoint.BatchedSnapshotCheckpointStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceStageHandler;
import com.fiberhome.ml.raha.job.stage.model.ResultPersistenceVerifier;
import com.fiberhome.ml.raha.job.stage.sample.SampleTaskStageHandler;
import com.fiberhome.ml.raha.job.stage.core.StageAttributeKeys;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.repository.port.SnapshotCheckpointRepository;
import com.fiberhome.ml.raha.repository.adapter.DefaultSnapshotCheckpointRepository;
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

    /** 分批生成单元格特征的服务。 */
    private final FeatureService featureService;
    /** 分批执行检测策略的服务。 */
    private final StrategyExecutionService executionService;
    /** 列内聚类服务。 */
    private final ColumnClusteringService clusteringService;
    /** 任务级采样服务。 */
    private final RahaSampleService sampleService;
    /** c1 采样宽表物化服务。 */
    private final SampleRecordService sampleRecordService;
    /** 可选采样物理结果回读验证器。 */
    private final ResultPersistenceVerifier resultPersistenceVerifier;
    /** 可选快照检查点仓储，用于训练复用采样前置产物。 */
    private final SnapshotCheckpointRepository snapshotCheckpointRepository;

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
                sampleRecordService, null,
                new DefaultSnapshotCheckpointRepository());
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
        this(datasetLoader, profileService, planService, executionService,
                featureService, clusteringService, sampleService,
                sampleRecordService, resultPersistenceVerifier,
                new DefaultSnapshotCheckpointRepository());
    }

    public SamplingWorkflow(RahaDatasetLoader datasetLoader,
                            ColumnProfileService profileService,
                            StrategyPlanService planService,
                            StrategyExecutionService executionService,
                            FeatureService featureService,
                            ColumnClusteringService clusteringService,
                            RahaSampleService sampleService,
                            SampleRecordService sampleRecordService,
                            ResultPersistenceVerifier resultPersistenceVerifier,
                            SnapshotCheckpointRepository snapshotCheckpointRepository) {
        super(datasetLoader, profileService, planService, executionService, featureService);
        if (clusteringService == null || sampleService == null
                || sampleRecordService == null) {
            throw new IllegalArgumentException("采样工作流业务依赖不能为空");
        }
        this.clusteringService = clusteringService;
        this.featureService = featureService;
        this.executionService = executionService;
        this.sampleService = sampleService;
        this.sampleRecordService = sampleRecordService;
        this.resultPersistenceVerifier = resultPersistenceVerifier;
        this.snapshotCheckpointRepository = snapshotCheckpointRepository;
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
        if (snapshotCheckpointRepository == null) {
            throw new IllegalStateException("采样工作流必须配置快照检查点仓储");
        }
        List<StageHandler> handlers = planningStages(request);
        handlers.add(new BatchedSnapshotCheckpointStageHandler(featureService,
                executionService, clusteringService, sampleService,
                snapshotCheckpointRepository,
                request.getSamplingRound(), request.getLabels()));
        handlers.add(new SampleTaskStageHandler(sampleService, sampleRecordService,
                request.getDataLoadRequest().getFormat(),
                request.getSamplingRound(), request.getLabels()));
        handlers.add(new ResultPersistenceStageHandler(
                StageAttributeKeys.SAMPLE_SERVICE_RESULT,
                resultPersistenceVerifier));
        return Collections.unmodifiableList(handlers);
    }
}
