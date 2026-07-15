package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.audit.RahaAuditAction;
import com.fiberhome.ml.raha.audit.RahaAuditService;
import com.fiberhome.ml.raha.audit.RahaAuditStatus;
import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.repository.DetectionResultRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * 为检测结果查询增加权限校验、数据集隔离和结果访问审计。
 */
public final class AuditedDetectionResultReader {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AuditedDetectionResultReader.class);
    /** 检测结果仓储。 */
    private final DetectionResultRepository repository;
    /** 结果访问控制器。 */
    private final RahaAccessController accessController;
    /** 结果访问审计服务。 */
    private final RahaAuditService auditService;
    /** 结果表或结果服务资源名称。 */
    private final String resultResourceName;

    public AuditedDetectionResultReader(DetectionResultRepository repository,
                                        RahaPermissionChecker permissionChecker,
                                        RahaAuditService auditService,
                                        String resultResourceName) {
        if (repository == null || permissionChecker == null || auditService == null) {
            throw new IllegalArgumentException("审计结果读取器依赖不能为空");
        }
        this.repository = repository;
        this.accessController = new RahaAccessController(permissionChecker);
        this.auditService = auditService;
        this.resultResourceName = ValueUtils.requireNotBlank(
                resultResourceName, "检测结果资源名称");
    }

    /**
     * 查询一个任务的全部检测结果，并校验每条结果所属数据集。
     */
    public List<DetectionResult> findByJob(String actor,
                                           String datasetId,
                                           String jobId) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "结果任务标识");
        authorize(actor, datasetId);
        try {
            List<DetectionResult> results = repository.findByJob(validatedJobId);
            for (DetectionResult result : results) {
                requireDataset(result, datasetId);
            }
            audit(actor, datasetId, validatedJobId, RahaAuditStatus.SUCCEEDED,
                    "检测任务结果访问成功");
            return results;
        } catch (RuntimeException exception) {
            LOGGER.error("检测任务结果访问失败，datasetId={}，jobId={}，resourceName={}",
                    datasetId, validatedJobId, resultResourceName, exception);
            audit(actor, datasetId, validatedJobId, RahaAuditStatus.FAILED,
                    "检测任务结果访问失败");
            throw exception;
        }
    }

    /**
     * 查询单个检测单元格结果，未命中时仍保留一次成功访问审计。
     */
    public Optional<DetectionResult> find(String actor,
                                          String datasetId,
                                          String jobId,
                                          String cellId) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "结果任务标识");
        String validatedCellId = ValueUtils.requireNotBlank(cellId, "结果单元格标识");
        authorize(actor, datasetId);
        try {
            Optional<DetectionResult> result = repository.find(
                    validatedJobId, validatedCellId);
            if (result.isPresent()) {
                requireDataset(result.get(), datasetId);
            }
            audit(actor, datasetId, validatedJobId, RahaAuditStatus.SUCCEEDED,
                    "单元格检测结果访问成功");
            return result;
        } catch (RuntimeException exception) {
            LOGGER.error("单元格检测结果访问失败，datasetId={}，jobId={}，resourceName={}",
                    datasetId, validatedJobId, resultResourceName, exception);
            audit(actor, datasetId, validatedJobId, RahaAuditStatus.FAILED,
                    "单元格检测结果访问失败");
            throw exception;
        }
    }

    private void authorize(String actor, String datasetId) {
        try {
            accessController.requireAllowed(new RahaPermissionRequest(actor,
                    RahaPermissionAction.READ, RahaResourceType.RESULT_DATA,
                    resultResourceName, datasetId));
        } catch (RahaAccessDeniedException exception) {
            audit(actor, datasetId, null, RahaAuditStatus.DENIED,
                    "检测结果访问权限校验拒绝");
            throw exception;
        }
    }

    private void audit(String actor,
                       String datasetId,
                       String jobId,
                       RahaAuditStatus status,
                       String summary) {
        auditService.record(actor, RahaAuditAction.RESULT_ACCESS, status,
                RahaResourceType.RESULT_DATA, resultResourceName, datasetId,
                jobId, null, summary);
    }

    private static void requireDataset(DetectionResult result, String datasetId) {
        if (result == null || !result.getCoordinate().getDatasetId().equals(datasetId)) {
            throw new IllegalStateException("检测结果所属数据集与访问上下文不一致");
        }
    }
}
