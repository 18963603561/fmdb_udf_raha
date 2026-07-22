package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.execution.RahaJobOrchestrator;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.job.stage.batch.ColumnBatchStageHandler;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatch;
import com.fiberhome.ml.raha.service.task.batch.ColumnBatchExecutionCoordinator;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统一提交并直接执行训练、检测或采样任务。
 */
public final class RahaTaskApplicationService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RahaTaskApplicationService.class);
    /** 通用任务生命周期编排器。 */
    private final RahaJobOrchestrator jobOrchestrator;
    /** 任务类型工作流注册器。 */
    private final RahaWorkflowRegistry workflowRegistry;
    /** 用于复用任务时查询已有阶段轨迹的仓储。 */
    private final StageRepository stageRepository;
    /** 可选最小任务请求工厂，默认运行时会自动提供。 */
    private final RahaTaskRequestFactory requestFactory;
    /** 默认运行时提供的 driver 侧列批协调器。 */
    private final ColumnBatchExecutionCoordinator columnBatchCoordinator;

    /**
     * 使用默认运行时装配创建任务应用服务。
     *
     * 该入口要求当前线程或应用上下文已经存在活动的 Spark 会话；没有活动会话时，
     * 构造阶段直接失败，避免在业务服务内部隐式创建 Spark 生命周期。
     */
    public RahaTaskApplicationService() {
        this(RahaTaskApplicationServiceFactory.createDefaultComponents());
    }

    public RahaTaskApplicationService(RahaJobOrchestrator jobOrchestrator,
                                      RahaWorkflowRegistry workflowRegistry,
                                      StageRepository stageRepository) {
        this(jobOrchestrator, workflowRegistry, stageRepository, null);
    }

    public RahaTaskApplicationService(RahaJobOrchestrator jobOrchestrator,
                                      RahaWorkflowRegistry workflowRegistry,
                                      StageRepository stageRepository,
                                      RahaTaskRequestFactory requestFactory) {
        this(jobOrchestrator, workflowRegistry, stageRepository,
                requestFactory, null);
    }

    RahaTaskApplicationService(
            RahaJobOrchestrator jobOrchestrator,
            RahaWorkflowRegistry workflowRegistry,
            StageRepository stageRepository,
            RahaTaskRequestFactory requestFactory,
            ColumnBatchExecutionCoordinator columnBatchCoordinator) {
        if (jobOrchestrator == null || workflowRegistry == null || stageRepository == null) {
            throw new IllegalArgumentException("统一任务应用服务依赖不能为空");
        }
        this.jobOrchestrator = jobOrchestrator;
        this.workflowRegistry = workflowRegistry;
        this.stageRepository = stageRepository;
        this.requestFactory = requestFactory;
        this.columnBatchCoordinator = columnBatchCoordinator;
    }

    /**
     * 接收默认工厂已经完成的组件装配结果。
     *
     * @param components 默认运行时组件
     */
    RahaTaskApplicationService(
            RahaTaskApplicationServiceFactory.DefaultComponents components) {
        this(components.getJobOrchestrator(), components.getWorkflowRegistry(),
                components.getStageRepository(), components.getRequestFactory(),
                components.getColumnBatchCoordinator());
    }

    /**
     * 获取与当前默认运行时共享仓储的最小请求工厂。
     *
     * @return 可创建采样、训练和检测最小请求的工厂
     */
    public RahaTaskRequestFactory getRequestFactory() {
        if (requestFactory == null) {
            throw new IllegalStateException("当前手工装配服务未提供最小任务请求工厂");
        }
        return requestFactory;
    }

    /**
     * 创建幂等任务并在当前进程立即执行对应工作流。
     *
     * @param request 类型化任务执行请求
     * @return 任务状态、阶段轨迹和业务输出
     */
    public RahaTaskExecutionResult execute(RahaTaskExecutionRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("统一任务执行请求不能为空");
        }
        LOGGER.info("开始统一执行 Raha 任务，jobType={}，datasetId={}，forceRun={}",
                request.getConfig().getJobType(), request.getConfig().getDatasetId(),
                Boolean.valueOf(request.isForceRun()));
        if (!request.isColumnBatchChild()
                && request.getColumnBatchOptions().isEnabled()) {
            if (columnBatchCoordinator == null) {
                throw new IllegalStateException("当前运行时未装配列批协调器");
            }
            if (columnBatchCoordinator.shouldBatch(request)) {
                return executeColumnBatches(request);
            }
        }
        return executeSingle(request);
    }

    /**
     * 执行不再拆分的普通任务或列批子任务。
     */
    private RahaTaskExecutionResult executeSingle(
            RahaTaskExecutionRequest request) {
        RahaJob job = jobOrchestrator.submit(request.getConfig());
        if (job.getStatus() != JobStatus.CREATED) {
            // 相同幂等任务已经执行或正在执行时返回已有状态，禁止重复运行阶段。
            LOGGER.info("复用已有 Raha 任务，jobId={}，status={}",
                    job.getJobId(), job.getStatus());
            return RahaTaskExecutionResult.reused(job,
                    stageRepository.findByJobId(job.getJobId()), request,
                    jobOrchestrator.findResultSummary(job));
        }
        RahaWorkflow workflow = workflowRegistry.require(job.getJobType());
        List<StageHandler> handlers = workflow.createStageHandlers(request);
        JobRunResult runResult = jobOrchestrator.execute(
                job, request.getConfig(), handlers);
        Map<String, Object> resultSummary =
                RahaTaskResultSummaryBuilder.build(request, runResult);
        jobOrchestrator.saveResultSummary(runResult.getJob(), resultSummary);
        LOGGER.info("统一 Raha 任务执行完成，jobId={}，status={}，stageCount={}",
                runResult.getJob().getJobId(), runResult.getJob().getStatus(),
                runResult.getStages().size());
        return RahaTaskExecutionResult.executed(runResult, resultSummary);
    }

    /**
     * 创建父任务并在单个编排阶段中执行全部列批子任务。
     */
    private RahaTaskExecutionResult executeColumnBatches(
            RahaTaskExecutionRequest request) {
        RahaJob parentJob = jobOrchestrator.submit(request.getConfig());
        if (parentJob.getStatus() != JobStatus.CREATED) {
            LOGGER.info("复用已有列批父任务，parentJobId={}，status={}",
                    parentJob.getJobId(), parentJob.getStatus());
            return RahaTaskExecutionResult.reused(parentJob,
                    stageRepository.findByJobId(parentJob.getJobId()), request,
                    jobOrchestrator.findResultSummary(parentJob));
        }
        List<ColumnBatch> batches = columnBatchCoordinator.plan(
                request, parentJob.getJobId());
        LOGGER.info("列批父任务已创建，parentJobId={}，batchCount={}，"
                        + "columnBatchSize={}",
                parentJob.getJobId(), batches.size(),
                request.getColumnBatchOptions().getColumnBatchSize());
        StageHandler handler = new ColumnBatchStageHandler(request, batches,
                columnBatchCoordinator, this::executeSingle);
        JobRunResult runResult = jobOrchestrator.execute(parentJob,
                request.getConfig(), Collections.singletonList(handler));
        Map<String, Object> resultSummary =
                RahaTaskResultSummaryBuilder.build(request, runResult);
        jobOrchestrator.saveResultSummary(runResult.getJob(), resultSummary);
        LOGGER.info("列批父任务执行完成，parentJobId={}，status={}，batchCount={}",
                runResult.getJob().getJobId(), runResult.getJob().getStatus(),
                batches.size());
        return RahaTaskExecutionResult.executed(runResult, resultSummary);
    }
}
