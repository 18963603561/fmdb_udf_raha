package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.sampling.AnnotationTask;

import java.util.List;
import java.util.Optional;

/**
 * 保存和读取元组标注任务及其状态。
 */
public interface AnnotationTaskRepository {

    void saveAll(String jobId,
                 List<AnnotationTask> tasks,
                 ArtifactVersion version,
                 long updatedAt);

    List<AnnotationTask> findByJob(String jobId);

    Optional<AnnotationTask> find(String jobId, String taskId);
}
