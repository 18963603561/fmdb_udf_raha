package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.job.RahaJob;

import java.util.List;

/**
 * 将任务状态和最终检测结果写入 FMDB 表。
 */
public interface FmdbResultWriter {

    long writeJob(String tableName, RahaJob job);

    long writeDetectionResults(String tableName,
                               String jobId,
                               List<DetectionResult> results);
}
