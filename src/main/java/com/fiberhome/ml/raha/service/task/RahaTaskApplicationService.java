package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.job.domain.JobRunResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.job.execution.RahaJobOrchestrator;
import com.fiberhome.ml.raha.job.stage.StageHandler;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import java.util.List;
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

    public RahaTaskApplicationService(RahaJobOrchestrator jobOrchestrator,
                                      RahaWorkflowRegistry workflowRegistry,
                                      StageRepository stageRepository) {
        if (jobOrchestrator == null || workflowRegistry == null || stageRepository == null) {
            throw new IllegalArgumentException("统一任务应用服务依赖不能为空");
        }
        this.jobOrchestrator = jobOrchestrator;
        this.workflowRegistry = workflowRegistry;
        this.stageRepository = stageRepository;
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
        LOGGER.info("开始统一执行 Raha 任务，jobType={}，datasetId={}",
                request.getConfig().getJobType(), request.getConfig().getDatasetId());
        RahaJob job = jobOrchestrator.submit(request.getConfig());
        if (job.getStatus() != JobStatus.CREATED) {
            // 相同幂等任务已经执行或正在执行时返回已有状态，禁止重复运行阶段。
            LOGGER.info("复用已有 Raha 任务，jobId={}，status={}",
                    job.getJobId(), job.getStatus());
            return RahaTaskExecutionResult.reused(job,
                    stageRepository.findByJobId(job.getJobId()));
        }
        RahaWorkflow workflow = workflowRegistry.require(job.getJobType());
        List<StageHandler> handlers = workflow.createStageHandlers(request);
        JobRunResult runResult = jobOrchestrator.execute(
                job, request.getConfig(), handlers);
        LOGGER.info("统一 Raha 任务执行完成，jobId={}，status={}，stageCount={}",
                runResult.getJob().getJobId(), runResult.getJob().getStatus(),
                runResult.getStages().size());
        return RahaTaskExecutionResult.executed(runResult);
    }
}
