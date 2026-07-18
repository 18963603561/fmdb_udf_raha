package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.sampling.AnnotationTask;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 基于统一仓储事务保存标注任务状态快照。
 */
public final class DefaultAnnotationTaskRepository implements AnnotationTaskRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultAnnotationTaskRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public void saveAll(String jobId,
                        List<AnnotationTask> tasks,
                        ArtifactVersion version,
                        long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (tasks == null || version == null) {
            throw new IllegalArgumentException("标注任务集合和版本不能为空");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (AnnotationTask task : tasks) {
                if (task == null || !validatedJobId.equals(task.getJobId())) {
                    throw new IllegalArgumentException("标注任务不属于当前 Raha 任务");
                }
                transactionRepository.save(new RepositoryRecord<AnnotationTask>(
                        new RepositoryKey(RepositoryNamespace.ANNOTATION_TASK,
                                validatedJobId, task.getTaskId()),
                        version, task.snapshot(), updatedAt));
            }
        });
    }

    @Override
    public List<AnnotationTask> findByJob(String jobId) {
        List<RepositoryRecord<AnnotationTask>> records = repository.findByPartition(
                RepositoryNamespace.ANNOTATION_TASK,
                ValueUtils.requireNotBlank(jobId, "任务标识"), AnnotationTask.class);
        List<AnnotationTask> tasks = new ArrayList<AnnotationTask>(records.size());
        for (RepositoryRecord<AnnotationTask> record : records) {
            tasks.add(record.getPayload().snapshot());
        }
        return Collections.unmodifiableList(tasks);
    }

    @Override
    public Optional<AnnotationTask> find(String jobId, String taskId) {
        Optional<RepositoryRecord<AnnotationTask>> record = repository.find(
                new RepositoryKey(RepositoryNamespace.ANNOTATION_TASK,
                        ValueUtils.requireNotBlank(jobId, "任务标识"),
                        ValueUtils.requireNotBlank(taskId, "标注任务标识")),
                AnnotationTask.class);
        return record.isPresent() ? Optional.of(record.get().getPayload().snapshot())
                : Optional.<AnnotationTask>empty();
    }
}
