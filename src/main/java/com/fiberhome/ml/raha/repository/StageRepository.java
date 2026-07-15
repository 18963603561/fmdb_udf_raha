package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.job.RahaStage;

import java.util.List;

/**
 * 提供任务阶段的类型化仓储接口。
 */
public interface StageRepository {

    SaveOutcome save(RahaStage stage, ArtifactVersion version, long updatedAt);

    List<RahaStage> findByJobId(String jobId);
}

