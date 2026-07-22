package com.fiberhome.ml.raha.data.profile;

import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.ColumnProfileRepository;
import java.time.Clock;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 编排列画像生成和按快照持久化。
 */
public final class ColumnProfileService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnProfileService.class);
    /** 列画像计算器。 */
    private final ColumnProfiler columnProfiler;
    /** 列画像仓储。 */
    private final ColumnProfileRepository profileRepository;
    /** 提供可测试时间的时钟。 */
    private final Clock clock;

    public ColumnProfileService(ColumnProfiler columnProfiler,
                                ColumnProfileRepository profileRepository,
                                Clock clock) {
        if (columnProfiler == null || profileRepository == null || clock == null) {
            throw new IllegalArgumentException("列画像服务依赖不能为空");
        }
        this.columnProfiler = columnProfiler;
        this.profileRepository = profileRepository;
        this.clock = clock;
    }

    /**
     * 生成并保存列画像，返回绑定画像的新数据集对象。
     *
     * @param dataset 已加载数据集
     * @param version 画像阶段版本
     * @return 绑定画像的新数据集
     */
    public RahaDataset profileAndSave(RahaDataset dataset, ArtifactVersion version) {
        return profileAndSave(dataset, version, false);
    }

    /**
     * 只画像并保存当前任务可检测字段。
     *
     * @param dataset 已加载数据集
     * @param version 画像阶段版本
     * @return 绑定当前字段画像的新数据集
     */
    public RahaDataset profileDetectableAndSave(
            RahaDataset dataset,
            ArtifactVersion version) {
        return profileAndSave(dataset, version, true);
    }

    private RahaDataset profileAndSave(RahaDataset dataset,
                                       ArtifactVersion version,
                                       boolean detectableOnly) {
        if (dataset == null || version == null) {
            throw new IllegalArgumentException("数据集和画像版本不能为空");
        }
        Map<String, ColumnProfile> profiles = detectableOnly
                ? columnProfiler.profileDetectable(dataset)
                : columnProfiler.profile(dataset);
        profileRepository.saveAll(dataset.getDatasetId(), dataset.getSnapshotId(),
                profiles, version, clock.millis());
        LOGGER.info("列画像持久化完成，datasetId={}，snapshotId={}，profileCount={}",
                dataset.getDatasetId(), dataset.getSnapshotId(), profiles.size());
        return dataset.withProfiles(profiles);
    }
}
