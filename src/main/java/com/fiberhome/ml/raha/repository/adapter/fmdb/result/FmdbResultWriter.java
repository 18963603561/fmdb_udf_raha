package com.fiberhome.ml.raha.repository.adapter.fmdb.result;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.job.domain.RahaJob;
import java.util.List;
import java.util.Map;

/**
 * 将任务状态和最终检测结果写入 FMDB 表。
 */
public interface FmdbResultWriter {

    long writeJob(String tableName,
                  RahaJob job,
                  Map<String, Object> resultSummary);

    long writeDetectionResults(String tableName,
                               FmdbDetectionWriteContext context,
                               List<DetectionResult> results);
}
