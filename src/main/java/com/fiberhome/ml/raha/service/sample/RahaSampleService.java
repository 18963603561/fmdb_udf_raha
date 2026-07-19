package com.fiberhome.ml.raha.service.sample;

import com.fiberhome.ml.raha.cluster.ColumnClusteringService;
import com.fiberhome.ml.raha.cluster.domain.ClusteringBatchResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.service.SamplingBatchResult;
import com.fiberhome.ml.raha.sampling.service.SamplingService;
import com.fiberhome.ml.raha.service.common.RahaServiceResult;
import com.fiberhome.ml.raha.data.type.JobStatus;
import com.fiberhome.ml.raha.service.common.RahaServiceSummary;
import com.fiberhome.ml.raha.data.type.JobType;
import java.time.Clock;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 编排列内聚类和覆盖采样并生成待标注任务。
 */
public final class RahaSampleService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaSampleService.class);
    /** 列内聚类服务。 */
    private final ColumnClusteringService clusteringService;
    /** 聚类覆盖采样服务。 */
    private final SamplingService samplingService;
    /** 提供可测试任务时间的时钟。 */
    private final Clock clock;

    public RahaSampleService(ColumnClusteringService clusteringService,
                             SamplingService samplingService,
                             Clock clock) {
        if (clusteringService == null || samplingService == null || clock == null) {
            throw new IllegalArgumentException("采样编排服务依赖不能为空");
        }
        this.clusteringService = clusteringService;
        this.samplingService = samplingService;
        this.clock = clock;
    }

    /**
     * 对当前特征重新聚类并按预算生成不重复待标注任务。
     *
     * @param request 采样服务输入
     * @return 统一采样任务结果
     */
    public RahaServiceResult<RahaSampleOutput> sample(RahaSampleRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("采样服务请求不能为空");
        }
        long startedAt = clock.millis();
        LOGGER.info("开始 Raha 采样服务，jobId={}，samplingRound={}，labelCount={}",
                request.getJobId(), request.getSamplingRound(), request.getLabels().size());
        try {
            ClusteringBatchResult clustering = request.getPreparedClustering();
            if (clustering == null) {
                clustering = clusteringService.clusterAndSaveParallel(
                        request.getJobId(), request.getFeatures(),
                        request.getClusteringConfig(), request.getRandomSeed(),
                        request.getArtifactVersion(),
                        request.getResourceConfig().getMaxParallelColumns(),
                        request.getResourceConfig().getStageTimeoutMillis());
            } else {
                LOGGER.info("采样服务复用聚类结果，jobId={}，samplingRound={}，assignmentCount={}",
                        request.getJobId(), request.getSamplingRound(),
                        clustering.getMetrics().getAssignmentCount());
            }
            SamplingBatchResult sampling = samplingService.createTasks(
                    request.getJobId(), request.getSamplingRound(), clustering,
                    request.getLabels(), request.getSamplingConfig(),
                    request.getRandomSeed(), request.getArtifactVersion());
            Map<String, String> details = new LinkedHashMap<String, String>();
            details.put("assignmentCount", String.valueOf(
                    clustering.getMetrics().getAssignmentCount()));
            details.put("candidateTupleCount", String.valueOf(
                    sampling.getMetrics().getCandidateTupleCount()));
            details.put("samplingVersion", sampling.getSamplingVersion());
            RahaServiceSummary summary = new RahaServiceSummary(startedAt, clock.millis(),
                    sampling.getMetrics().getCandidateTupleCount(),
                    sampling.getTasks().size(),
                    Math.max(0L, sampling.getMetrics().getCandidateTupleCount()
                            - sampling.getTasks().size()), 0L, details);
            LOGGER.info("Raha 采样服务完成，jobId={}，samplingRound={}，taskCount={}",
                    request.getJobId(), request.getSamplingRound(),
                    sampling.getTasks().size());
            return new RahaServiceResult<RahaSampleOutput>(request.getJobId(),
                    JobType.SAMPLING, JobStatus.SUCCEEDED,
                    "memory://annotation-task/" + request.getJobId(), summary,
                    new RahaSampleOutput(clustering, sampling), null, null);
        } catch (RuntimeException exception) {
            // 聚类或标注任务仓储异常统一转换为失败结果，并保留完整异常堆栈。
            LOGGER.error("Raha 采样服务失败，jobId={}，samplingRound={}",
                    request.getJobId(), request.getSamplingRound(), exception);
            RahaServiceSummary summary = new RahaServiceSummary(startedAt, clock.millis(),
                    1L, 0L, 0L, 1L, Collections.<String, String>emptyMap());
            return new RahaServiceResult<RahaSampleOutput>(request.getJobId(),
                    JobType.SAMPLING, JobStatus.FAILED, null, summary, null,
                    "SAMPLE_SERVICE_FAILED", exception.getClass().getSimpleName());
        }
    }

    /**
     * 在外部标注成功后完成并持久化标注任务。
     *
     * @param jobId 任务标识
     * @param task 本轮待标注任务
     * @param version 仓储业务版本
     * @return 已进入完成状态的任务快照
     */
    public AnnotationTask completeTask(String jobId,
                                       AnnotationTask task,
                                       ArtifactVersion version) {
        return samplingService.completeTask(jobId, task, version);
    }
}
