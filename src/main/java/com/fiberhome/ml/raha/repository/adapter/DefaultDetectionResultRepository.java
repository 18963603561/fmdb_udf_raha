package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.core.RepositoryKey;
import com.fiberhome.ml.raha.repository.core.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.core.RepositoryRecord;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 基于统一仓储事务保存检测结果。
 */
public final class DefaultDetectionResultRepository implements DetectionResultRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultDetectionResultRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public void saveAll(String jobId,
                        List<DetectionResult> results,
                        ArtifactVersion version,
                        long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (results == null || version == null) {
            throw new IllegalArgumentException("检测结果集合和版本不能为空");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (DetectionResult result : results) {
                transactionRepository.save(new RepositoryRecord<DetectionResult>(
                        new RepositoryKey(RepositoryNamespace.DETECTION_RESULT,
                                validatedJobId, result.getCoordinate().toCellId()),
                        version, result, updatedAt));
            }
        });
    }

    @Override
    public List<DetectionResult> findByJob(String jobId) {
        List<RepositoryRecord<DetectionResult>> records = repository.findByPartition(
                RepositoryNamespace.DETECTION_RESULT,
                ValueUtils.requireNotBlank(jobId, "任务标识"), DetectionResult.class);
        List<DetectionResult> results = new ArrayList<DetectionResult>(records.size());
        for (RepositoryRecord<DetectionResult> record : records) {
            results.add(record.getPayload());
        }
        return Collections.unmodifiableList(results);
    }

    @Override
    public Optional<DetectionResult> find(String jobId, String cellId) {
        Optional<RepositoryRecord<DetectionResult>> record = repository.find(
                new RepositoryKey(RepositoryNamespace.DETECTION_RESULT,
                        ValueUtils.requireNotBlank(jobId, "任务标识"),
                        ValueUtils.requireNotBlank(cellId, "单元格标识")),
                DetectionResult.class);
        return record.isPresent() ? Optional.of(record.get().getPayload())
                : Optional.<DetectionResult>empty();
    }
}
