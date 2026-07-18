package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.job.domain.RahaStage;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import java.util.List;

/**
 * 提供任务阶段的类型化仓储接口。
 */
public interface StageRepository {

    SaveOutcome save(RahaStage stage, ArtifactVersion version, long updatedAt);

    List<RahaStage> findByJobId(String jobId);
}

