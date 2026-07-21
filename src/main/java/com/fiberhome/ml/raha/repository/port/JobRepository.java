package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.job.domain.RahaJob;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import java.util.Map;
import java.util.Optional;

/**
 * 提供任务状态的类型化仓储接口。
 */
public interface JobRepository {

    SaveOutcome save(RahaJob job, long updatedAt);

    /**
     * 保存任务状态和可选结果摘要。
     *
     * <p>默认仓储可以忽略摘要，FMDB 仓储会把摘要写入 result_summary_json 供复用回填。</p>
     *
     * @param job 任务快照
     * @param updatedAt 更新时间
     * @param resultSummary 结果摘要
     * @return 保存结果
     */
    default SaveOutcome save(RahaJob job,
                             long updatedAt,
                             Map<String, Object> resultSummary) {
        return save(job, updatedAt);
    }

    Optional<RahaJob> findByIdempotentKey(String datasetId, String idempotentKey);

    /**
     * 查询同一幂等任务的最新结果摘要。
     *
     * @param datasetId 数据集标识
     * @param idempotentKey 幂等键
     * @return 结果摘要，不存在时为空
     */
    default Optional<Map<String, Object>> findResultSummaryByIdempotentKey(
            String datasetId,
            String idempotentKey) {
        return Optional.empty();
    }
}
