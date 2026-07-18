package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 提供列画像按数据集和快照保存、读取的接口。
 */
public interface ColumnProfileRepository {

    SaveOutcome save(String datasetId,
                     String snapshotId,
                     ColumnProfile profile,
                     ArtifactVersion version,
                     long updatedAt);

    void saveAll(String datasetId,
                 String snapshotId,
                 Map<String, ColumnProfile> profiles,
                 ArtifactVersion version,
                 long updatedAt);

    Optional<ColumnProfile> find(String datasetId, String snapshotId, String columnName);

    List<ColumnProfile> findBySnapshot(String datasetId, String snapshotId);
}
