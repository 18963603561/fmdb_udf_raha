package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import java.util.List;
import java.util.Optional;

/**
 * 持久化和查询单元格检测结果的仓储契约。
 */
public interface DetectionResultRepository {

    long saveAll(String jobId,
                 List<DetectionResult> results,
                 ArtifactVersion version,
                 long updatedAt);

    /**
     * 使用可信输入上下文保存检测结果；默认实现兼容现有内存仓储。
     *
     * @param context 检测写入上下文
     * @param results 全量预测结果
     * @param version 结果版本
     * @param updatedAt 更新时间
     * @return 对外可观测的错误结果写入数量
     */
    default long saveAll(DetectionResultSaveContext context,
                         List<DetectionResult> results,
                         ArtifactVersion version,
                         long updatedAt) {
        if (context == null) {
            throw new IllegalArgumentException("检测写入上下文不能为空");
        }
        return saveAll(context.getJobId(), results, version, updatedAt);
    }

    List<DetectionResult> findByJob(String jobId);

    Optional<DetectionResult> find(String jobId, String cellId);
}
