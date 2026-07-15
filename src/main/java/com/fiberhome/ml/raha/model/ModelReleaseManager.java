package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ModelStatus;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
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

    public ModelReleaseManager(ModelMetadataRepository repository, Clock clock) {
        if (repository == null || clock == null) {
            throw new IllegalArgumentException("模型发布管理器依赖不能为空");
        }
        this.repository = repository;
        this.clock = clock;
    }

    public RahaColumnModel markCandidate(RahaColumnModel model,
                                         ArtifactVersion version) {
        LOGGER.info("开始标记候选模型，datasetId={}，columnName={}，modelVersion={}",
                model == null ? null : model.getDatasetId(),
                model == null ? null : model.getColumnName(),
                model == null ? null : model.getModelVersion());
        requireModelFile(model);
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
