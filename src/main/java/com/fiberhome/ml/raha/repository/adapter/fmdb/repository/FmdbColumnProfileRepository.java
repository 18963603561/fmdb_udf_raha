package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbColumnProfileCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.repository.port.ColumnProfileRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在任务执行期间暂存画像，并从最终训练列级产物恢复已物化画像。
 *
 * <p>列级物理表要求一批一列一条完整记录，因此阶段内保存不能提前追加残缺行；
 * 最终写入由训练产物物化服务统一完成。</p>
 */
public final class FmdbColumnProfileRepository
        implements ColumnProfileRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbColumnProfileRepository.class);
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 当前进程尚未统一物化的画像。 */
    private final Map<String, PendingProfile> pendingProfiles =
            new LinkedHashMap<String, PendingProfile>();
    /** 训练列级产物表名。 */
    private final String tableName;

    public FmdbColumnProfileRepository(FmdbTableGateway tableGateway,
                                       FmdbPersistenceConfig persistenceConfig) {
        if (tableGateway == null || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 列画像仓储依赖不能为空");
        }
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT.getTableName();
    }

    @Override
    public synchronized SaveOutcome save(String datasetId,
                                         String snapshotId,
                                         ColumnProfile profile,
                                         ArtifactVersion version,
                                         long updatedAt) {
        if (profile == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("列画像、结果版本和更新时间必须有效");
        }
        String key = key(datasetId, snapshotId, profile.getColumnName());
        PendingProfile previous = pendingProfiles.get(key);
        if (previous != null && previous.version.equals(version)) {
            return SaveOutcome.UNCHANGED;
        }
        pendingProfiles.put(key, new PendingProfile(profile, version));
        LOGGER.debug("训练列画像已进入统一物化缓冲，datasetId={}，snapshotId={}，columnName={}",
                datasetId, snapshotId, profile.getColumnName());
        return previous == null ? SaveOutcome.CREATED : SaveOutcome.UPDATED;
    }

    @Override
    public synchronized void saveAll(String datasetId,
                                     String snapshotId,
                                     Map<String, ColumnProfile> profiles,
                                     ArtifactVersion version,
                                     long updatedAt) {
        if (profiles == null || profiles.isEmpty()) {
            throw new IllegalArgumentException("列画像集合不能为空");
        }
        for (ColumnProfile profile : profiles.values()) {
            save(datasetId, snapshotId, profile, version, updatedAt);
        }
        LOGGER.info("训练列画像阶段保存完成，datasetId={}，snapshotId={}，profileCount={}",
                datasetId, snapshotId, profiles.size());
    }

    @Override
    public synchronized Optional<ColumnProfile> find(String datasetId,
                                                     String snapshotId,
                                                     String columnName) {
        String key = key(datasetId, snapshotId, columnName);
        PendingProfile pending = pendingProfiles.get(key);
        if (pending != null) {
            return Optional.of(pending.profile);
        }
        for (ColumnProfile profile : physicalProfiles(datasetId, snapshotId)) {
            if (profile.getColumnName().equals(columnName)) {
                return Optional.of(profile);
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized List<ColumnProfile> findBySnapshot(String datasetId,
                                                           String snapshotId) {
        Map<String, ColumnProfile> profiles = new LinkedHashMap<String, ColumnProfile>();
        for (ColumnProfile profile : physicalProfiles(datasetId, snapshotId)) {
            profiles.put(profile.getColumnName(), profile);
        }
        String prefix = snapshotKey(datasetId, snapshotId);
        for (Map.Entry<String, PendingProfile> entry : pendingProfiles.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                profiles.put(entry.getValue().profile.getColumnName(),
                        entry.getValue().profile);
            }
        }
        List<ColumnProfile> result = new ArrayList<ColumnProfile>(profiles.values());
        Collections.sort(result, Comparator.comparing(ColumnProfile::getColumnName));
        return Collections.unmodifiableList(result);
    }

    private List<ColumnProfile> physicalProfiles(String datasetId,
                                                 String snapshotId) {
        String validatedDataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String validatedSnapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        LOGGER.debug("训练列画像仅使用当前任务缓存，未命中缓存，datasetId={}，snapshotId={}",
                validatedDataset, validatedSnapshot);
        return Collections.emptyList();
    }

    private static long number(Row row, String column) {
        return ((Number) row.getAs(column)).longValue();
    }

    private static String key(String datasetId, String snapshotId, String columnName) {
        return snapshotKey(datasetId, snapshotId)
                + ValueUtils.requireNotBlank(columnName, "字段名称");
    }

    private static String snapshotKey(String datasetId, String snapshotId) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "数据集标识");
        String snapshot = ValueUtils.requireNotBlank(snapshotId, "快照标识");
        return dataset.length() + ":" + dataset + snapshot.length() + ":" + snapshot + ":";
    }

    /** 尚未进入完整训练列级记录的画像和业务版本。 */
    private static final class PendingProfile {

        /** 列画像。 */
        private final ColumnProfile profile;
        /** 阶段业务版本。 */
        private final ArtifactVersion version;

        private PendingProfile(ColumnProfile profile, ArtifactVersion version) {
            this.profile = profile;
            this.version = version;
        }
    }
}
