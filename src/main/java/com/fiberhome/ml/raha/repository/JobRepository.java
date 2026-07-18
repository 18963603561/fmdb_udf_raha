package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.job.RahaJob;

import java.util.Optional;

/**
 * 提供任务状态的类型化仓储接口。
 */
public interface JobRepository {

    SaveOutcome save(RahaJob job, long updatedAt);

    Optional<RahaJob> findByIdempotentKey(String datasetId, String idempotentKey);
}

