package com.fiberhome.ml.raha.data.profile;

import com.fiberhome.ml.raha.data.ColumnProfile;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.ColumnProfileRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;

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
        if (dataset == null || version == null) {
            throw new IllegalArgumentException("数据集和画像版本不能为空");
        }
        Map<String, ColumnProfile> profiles = columnProfiler.profile(dataset);
        profileRepository.saveAll(dataset.getDatasetId(), dataset.getSnapshotId(),
                profiles, version, clock.millis());
        LOGGER.info("列画像持久化完成，datasetId={}，snapshotId={}，profileCount={}",
                dataset.getDatasetId(), dataset.getSnapshotId(), profiles.size());
        return dataset.withProfiles(profiles);
    }
}
