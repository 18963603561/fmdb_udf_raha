package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.job.execution.RahaJobOrchestrator;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import java.util.Arrays;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 统一创建 Raha 任务应用服务的工厂类。
 *
 * 该工厂只负责把上层装配好的编排器、工作流注册器和阶段仓储组装成
 * `RahaTaskApplicationService`，不在这里隐藏底层默认实现选择。
 */
public final class RahaTaskApplicationServiceFactory {

    /** 日志记录器。*/
    private static final Logger LOGGER = LoggerFactory.getLogger(
            RahaTaskApplicationServiceFactory.class);

    private RahaTaskApplicationServiceFactory() {
    }

    /**
     * 直接使用已经创建好的工作流注册器生成统一入口服务。
     *
     * @param jobOrchestrator 任务编排器
     * @param workflowRegistry 工作流注册器
     * @param stageRepository 阶段仓储
     * @return 统一任务应用服务
     */
    public static RahaTaskApplicationService create(
            RahaJobOrchestrator jobOrchestrator,
            RahaWorkflowRegistry workflowRegistry,
            StageRepository stageRepository) {
        if (jobOrchestrator == null || workflowRegistry == null
                || stageRepository == null) {
            throw new IllegalArgumentException("Raha 任务应用服务工厂依赖不能为空");
        }
        LOGGER.info("创建 RahaTaskApplicationService，workflowRegistry={}",
                workflowRegistry.getClass().getSimpleName());
        return new RahaTaskApplicationService(jobOrchestrator, workflowRegistry,
                stageRepository);
    }

    /**
     * 使用工作流集合自动创建工作流注册器，再生成统一入口服务。
     *
     * @param jobOrchestrator 任务编排器
     * @param stageRepository 阶段仓储
     * @param workflows 工作流集合
     * @return 统一任务应用服务
     */
    public static RahaTaskApplicationService create(
            RahaJobOrchestrator jobOrchestrator,
            StageRepository stageRepository,
            Collection<RahaWorkflow> workflows) {
        if (workflows == null || workflows.isEmpty()) {
            throw new IllegalArgumentException("Raha 工作流集合不能为空");
        }
        return create(jobOrchestrator, new RahaWorkflowRegistry(workflows),
                stageRepository);
    }

    /**
     * 使用工作流数组自动创建工作流注册器，再生成统一入口服务。
     *
     * @param jobOrchestrator 任务编排器
     * @param stageRepository 阶段仓储
     * @param workflows 工作流数组
     * @return 统一任务应用服务
     */
    @SafeVarargs
    public static RahaTaskApplicationService create(
            RahaJobOrchestrator jobOrchestrator,
            StageRepository stageRepository,
            RahaWorkflow... workflows) {
        if (workflows == null || workflows.length == 0) {
            throw new IllegalArgumentException("Raha 工作流数组不能为空");
        }
        return create(jobOrchestrator, stageRepository, Arrays.asList(workflows));
    }
}
