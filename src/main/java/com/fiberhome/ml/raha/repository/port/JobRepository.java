package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import java.util.Optional;

/**
 * 提供任务状态的类型化仓储接口。
 */
public interface JobRepository {

    SaveOutcome save(RahaJob job, long updatedAt);

    Optional<RahaJob> findByIdempotentKey(String datasetId, String idempotentKey);
}

