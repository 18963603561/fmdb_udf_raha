package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.execution.RahaJobOrchestrator;
import com.fiberhome.ml.raha.job.stage.core.StageHandler;
import com.fiberhome.ml.raha.repository.port.StageRepository;
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
        if (jobOrchestrator == null || workflowRegistry == null || stageRepository == null) {
            throw new IllegalArgumentException("统一任务应用服务依赖不能为空");
        }
        this.jobOrchestrator = jobOrchestrator;
        this.workflowRegistry = workflowRegistry;
        this.stageRepository = stageRepository;
        this.requestFactory = requestFactory;
    }

    /**
     * 接收默认工厂已经完成的组件装配结果。
     *
     * @param components 默认运行时组件
     */
    RahaTaskApplicationService(
            RahaTaskApplicationServiceFactory.DefaultComponents components) {
        this(components.getJobOrchestrator(), components.getWorkflowRegistry(),
                components.getStageRepository(), components.getRequestFactory());
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
}
