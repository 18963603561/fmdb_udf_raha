package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.data.DetectionResult;

import java.util.List;
import java.util.Optional;

/**
 * 持久化和查询单元格检测结果的仓储契约。
 */
public interface DetectionResultRepository {

    void saveAll(String jobId,
                 List<DetectionResult> results,
                 ArtifactVersion version,
                 long updatedAt);

    List<DetectionResult> findByJob(String jobId);

    Optional<DetectionResult> find(String jobId, String cellId);
}
