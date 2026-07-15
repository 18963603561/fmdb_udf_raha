package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.audit.RahaAuditAction;
import com.fiberhome.ml.raha.audit.RahaAuditService;
import com.fiberhome.ml.raha.audit.RahaAuditStatus;
import com.fiberhome.ml.raha.data.ModelStatus;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
import com.fiberhome.ml.raha.security.AllowAllRahaPermissionChecker;
import com.fiberhome.ml.raha.security.RahaAccessController;
import com.fiberhome.ml.raha.security.RahaAccessDeniedException;
import com.fiberhome.ml.raha.security.RahaPermissionAction;
import com.fiberhome.ml.raha.security.RahaPermissionChecker;
import com.fiberhome.ml.raha.security.RahaPermissionRequest;
import com.fiberhome.ml.raha.security.RahaResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 管理模型候选、发布、停用和向前一个已发布版本回滚。
 */
public final class ModelReleaseManager {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(ModelReleaseManager.class);
    /** 模型元数据仓储。 */
    private final ModelMetadataRepository repository;
    /** 提供可测试状态更新时间的时钟。 */
    private final Clock clock;
    /** 模型操作访问控制器。 */
    private final RahaAccessController accessController;
    /** 模型生命周期审计服务。 */
    private final RahaAuditService auditService;

    public ModelReleaseManager(ModelMetadataRepository repository, Clock clock) {
        this(repository, clock, AllowAllRahaPermissionChecker.getInstance(),
                RahaAuditService.noOp(clock));
    }

    public ModelReleaseManager(ModelMetadataRepository repository,
                               Clock clock,
                               RahaPermissionChecker permissionChecker,
                               RahaAuditService auditService) {
        if (repository == null || clock == null || permissionChecker == null
                || auditService == null) {
            throw new IllegalArgumentException("模型发布管理器依赖不能为空");
        }
        this.repository = repository;
        this.clock = clock;
        this.accessController = new RahaAccessController(permissionChecker);
        this.auditService = auditService;
    }

    public RahaColumnModel markCandidate(RahaColumnModel model,
                                         ArtifactVersion version) {
        LOGGER.info("开始标记候选模型，datasetId={}，columnName={}，modelVersion={}",
                model == null ? null : model.getDatasetId(),
                model == null ? null : model.getColumnName(),
                model == null ? null : model.getModelVersion());
        requireModelFile(model);
        Double qualityGatePassed = model.getMetrics().get("qualityGatePassed");
        // 新训练链路写入质量门禁结论，未通过的模型禁止进入候选状态。
        if (qualityGatePassed != null && qualityGatePassed < 1.0d) {
            throw new IllegalStateException("模型质量门禁未通过，禁止标记候选模型");
        }
        RahaColumnModel candidate = model.withStatus(ModelStatus.CANDIDATE);
        repository.saveAll(Collections.singletonList(candidate), version, clock.millis());
        LOGGER.info("候选模型标记完成，datasetId={}，columnName={}，modelVersion={}",
                candidate.getDatasetId(), candidate.getColumnName(), candidate.getModelVersion());
        return candidate;
    }

    public RahaColumnModel publish(String datasetId,
                                   String columnName,
                                   String modelVersion,
                                   ArtifactVersion version) {
        return publish(datasetId, columnName, modelVersion, version, "SYSTEM");
    }

    public RahaColumnModel publish(String datasetId,
                                   String columnName,
                                   String modelVersion,
                                   ArtifactVersion version,
                                   String actor) {
        String resourceName = modelResource(columnName, modelVersion);
        try {
            authorize(actor, RahaPermissionAction.PUBLISH, resourceName, datasetId);
            RahaColumnModel published = publishAuthorized(
                    datasetId, columnName, modelVersion, version);
            auditModel(actor, RahaAuditAction.MODEL_PUBLISH,
                    RahaAuditStatus.SUCCEEDED, resourceName, datasetId,
                    modelVersion, "列级模型发布成功");
            return published;
        } catch (RahaAccessDeniedException exception) {
            auditModel(actor, RahaAuditAction.MODEL_PUBLISH,
                    RahaAuditStatus.DENIED, resourceName, datasetId,
                    modelVersion, "列级模型发布权限校验拒绝");
            throw exception;
        } catch (RuntimeException exception) {
            auditFailureSafely(actor, RahaAuditAction.MODEL_PUBLISH,
                    resourceName, datasetId, modelVersion, "列级模型发布失败");
            throw exception;
        }
    }

    private RahaColumnModel publishAuthorized(String datasetId,
                                              String columnName,
                                              String modelVersion,
                                              ArtifactVersion version) {
        LOGGER.info("开始发布列级模型，datasetId={}，columnName={}，modelVersion={}",
                datasetId, columnName, modelVersion);
        RahaColumnModel target = repository.find(datasetId, columnName, modelVersion)
                .orElseThrow(() -> new IllegalArgumentException("待发布模型不存在"));
        if (target.getStatus() != ModelStatus.CANDIDATE) {
            throw new IllegalStateException("只有候选模型可以首次发布");
        }
        requireModelFile(target);
        List<RahaColumnModel> updates = new ArrayList<RahaColumnModel>();
        repository.findPublished(datasetId, columnName).ifPresent(
                current -> updates.add(current.withStatus(
                        ModelStatus.DISABLED, clock.millis())));
        RahaColumnModel published = target.withStatus(ModelStatus.PUBLISHED, clock.millis());
        updates.add(published);
        repository.saveAll(updates, version, clock.millis());
        LOGGER.info("列级模型发布完成，datasetId={}，columnName={}，modelVersion={}",
                datasetId, columnName, modelVersion);
        return published;
    }

    public RahaColumnModel disable(String datasetId,
                                   String columnName,
                                   String modelVersion,
                                   ArtifactVersion version) {
        return disable(datasetId, columnName, modelVersion, version, "SYSTEM");
    }

    public RahaColumnModel disable(String datasetId,
                                   String columnName,
                                   String modelVersion,
                                   ArtifactVersion version,
                                   String actor) {
        String resourceName = modelResource(columnName, modelVersion);
        try {
            authorize(actor, RahaPermissionAction.DISABLE, resourceName, datasetId);
            RahaColumnModel disabled = disableAuthorized(
                    datasetId, columnName, modelVersion, version);
            auditModel(actor, RahaAuditAction.MODEL_DISABLE,
                    RahaAuditStatus.SUCCEEDED, resourceName, datasetId,
                    modelVersion, "列级模型停用成功");
            return disabled;
        } catch (RahaAccessDeniedException exception) {
            auditModel(actor, RahaAuditAction.MODEL_DISABLE,
                    RahaAuditStatus.DENIED, resourceName, datasetId,
                    modelVersion, "列级模型停用权限校验拒绝");
            throw exception;
        } catch (RuntimeException exception) {
            auditFailureSafely(actor, RahaAuditAction.MODEL_DISABLE,
                    resourceName, datasetId, modelVersion, "列级模型停用失败");
            throw exception;
        }
    }

    private RahaColumnModel disableAuthorized(String datasetId,
                                              String columnName,
                                              String modelVersion,
                                              ArtifactVersion version) {
        LOGGER.info("开始停用列级模型，datasetId={}，columnName={}，modelVersion={}",
                datasetId, columnName, modelVersion);
        RahaColumnModel model = repository.find(datasetId, columnName, modelVersion)
                .orElseThrow(() -> new IllegalArgumentException("待停用模型不存在"));
        RahaColumnModel disabled = model.withStatus(ModelStatus.DISABLED, clock.millis());
        repository.saveAll(Collections.singletonList(disabled), version, clock.millis());
        LOGGER.info("列级模型停用完成，datasetId={}，columnName={}，modelVersion={}",
                datasetId, columnName, modelVersion);
        return disabled;
    }

    public RahaColumnModel rollback(String datasetId,
                                    String columnName,
                                    ArtifactVersion version) {
        return rollback(datasetId, columnName, version, "SYSTEM");
    }

    public RahaColumnModel rollback(String datasetId,
                                    String columnName,
                                    ArtifactVersion version,
                                    String actor) {
        String resourceName = modelResource(columnName, "published-history");
        try {
            authorize(actor, RahaPermissionAction.ROLLBACK, resourceName, datasetId);
            RahaColumnModel restored = rollbackAuthorized(datasetId, columnName, version);
            auditModel(actor, RahaAuditAction.MODEL_ROLLBACK,
                    RahaAuditStatus.SUCCEEDED, resourceName, datasetId,
                    restored.getModelVersion(), "列级模型回滚成功");
            return restored;
        } catch (RahaAccessDeniedException exception) {
            auditModel(actor, RahaAuditAction.MODEL_ROLLBACK,
                    RahaAuditStatus.DENIED, resourceName, datasetId,
                    null, "列级模型回滚权限校验拒绝");
            throw exception;
        } catch (RuntimeException exception) {
            auditFailureSafely(actor, RahaAuditAction.MODEL_ROLLBACK,
                    resourceName, datasetId, null, "列级模型回滚失败");
            throw exception;
        }
    }

    private RahaColumnModel rollbackAuthorized(String datasetId,
                                               String columnName,
                                               ArtifactVersion version) {
        LOGGER.info("开始回滚列级模型，datasetId={}，columnName={}", datasetId, columnName);
        RahaColumnModel current = repository.findPublished(datasetId, columnName)
                .orElseThrow(() -> new IllegalStateException("当前字段没有已发布模型"));
        List<RahaColumnModel> candidates = new ArrayList<RahaColumnModel>();
        for (RahaColumnModel model : repository.findByColumn(datasetId, columnName)) {
            // 回滚只允许切换到首次发布时间更早的历史发布版本，排除从未上线的停用草稿。
            if (model.getStatus() == ModelStatus.DISABLED
                    && model.getPublishedAt() != null
                    && isPublishedBefore(model, current)) {
                candidates.add(model);
            }
        }
        if (candidates.isEmpty()) {
            throw new IllegalStateException("不存在可回滚的更早已发布模型");
        }
        Collections.sort(candidates, Comparator
                .comparing(RahaColumnModel::getPublishedAt)
                .thenComparingLong(RahaColumnModel::getCreatedAt)
                .reversed());
        RahaColumnModel previous = candidates.get(0);
        requireModelFile(previous);
        RahaColumnModel disabledCurrent = current.withStatus(
                ModelStatus.DISABLED, clock.millis());
        RahaColumnModel restored = previous.withStatus(
                ModelStatus.PUBLISHED, clock.millis());
        repository.saveAll(java.util.Arrays.asList(disabledCurrent, restored),
                version, clock.millis());
        LOGGER.info("列级模型回滚完成，datasetId={}，columnName={}，fromVersion={}，toVersion={}",
                datasetId, columnName, current.getModelVersion(), restored.getModelVersion());
        return restored;
    }

    private void authorize(String actor,
                           RahaPermissionAction action,
                           String resourceName,
                           String datasetId) {
        accessController.requireAllowed(new RahaPermissionRequest(actor, action,
                RahaResourceType.MODEL, resourceName, datasetId));
    }

    private void auditModel(String actor,
                            RahaAuditAction action,
                            RahaAuditStatus status,
                            String resourceName,
                            String datasetId,
                            String modelVersion,
                            String summary) {
        auditService.record(actor, action, status, RahaResourceType.MODEL,
                resourceName, datasetId, null, modelVersion, summary);
    }

    private void auditFailureSafely(String actor,
                                    RahaAuditAction action,
                                    String resourceName,
                                    String datasetId,
                                    String modelVersion,
                                    String summary) {
        try {
            auditModel(actor, action, RahaAuditStatus.FAILED, resourceName,
                    datasetId, modelVersion, summary);
        } catch (RuntimeException auditException) {
            LOGGER.error("模型操作失败审计写入异常，action={}，datasetId={}，resourceName={}",
                    action, datasetId, resourceName, auditException);
        }
    }

    private static String modelResource(String columnName, String modelVersion) {
        return columnName + ":" + modelVersion;
    }

    private static boolean isPublishedBefore(RahaColumnModel candidate,
                                             RahaColumnModel current) {
        int publishedTimeCompare = candidate.getPublishedAt().compareTo(
                current.getPublishedAt());
        return publishedTimeCompare < 0 || (publishedTimeCompare == 0
                && candidate.getCreatedAt() < current.getCreatedAt());
    }

    private static void requireModelFile(RahaColumnModel model) {
        if (model == null || model.getModelPath() == null
                || model.getModelPath().trim().isEmpty()) {
            throw new IllegalStateException("候选或发布模型必须包含可加载模型文件");
        }
    }
}
