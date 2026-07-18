package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.checkpoint.StageCheckpoint;
import com.fiberhome.ml.raha.data.type.StageType;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import java.util.List;
import java.util.Optional;

/**
 * 保存阶段检查点尝试并查找输入版本一致的可复用成功结果。
 */
public interface StageCheckpointRepository {

    SaveOutcome save(StageCheckpoint checkpoint, long updatedAt);

    List<StageCheckpoint> findAttempts(String jobId, StageType stageType);

    Optional<StageCheckpoint> findReusable(String jobId,
                                           StageType stageType,
                                           ArtifactVersion inputVersion,
                                           String inputFingerprint);
}
