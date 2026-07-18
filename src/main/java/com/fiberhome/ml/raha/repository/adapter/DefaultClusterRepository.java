package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.core.RepositoryKey;
import com.fiberhome.ml.raha.repository.core.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.core.RepositoryRecord;
import com.fiberhome.ml.raha.repository.port.ClusterRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * 基于统一仓储事务保存聚类结果和成员映射。
 */
public final class DefaultClusterRepository implements ClusterRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultClusterRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public void save(String jobId,
                     ColumnClusteringResult result,
                     ArtifactVersion version,
                     long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (result == null || version == null) {
            throw new IllegalArgumentException("聚类结果和版本不能为空");
        }
        repository.executeInTransaction(transactionRepository -> {
            transactionRepository.save(new RepositoryRecord<ColumnClusteringResult>(
                    new RepositoryKey(RepositoryNamespace.CLUSTER_RUN_SUMMARY,
                            summaryPartition(validatedJobId, result.getColumnName()),
                            result.getClusterVersion()),
                    version, result, updatedAt));
            for (ClusterAssignment assignment : result.getAssignments()) {
                transactionRepository.save(new RepositoryRecord<ClusterAssignment>(
                        new RepositoryKey(RepositoryNamespace.CLUSTER_ASSIGNMENT,
                                assignmentPartition(validatedJobId, result.getColumnName(),
                                        result.getClusterVersion()), assignment.getCellId()),
                        version, assignment, updatedAt));
            }
        });
    }

    @Override
    public Optional<ColumnClusteringResult> findResult(String jobId,
                                                       String columnName,
                                                       String clusterVersion) {
        Optional<RepositoryRecord<ColumnClusteringResult>> record = repository.find(
                new RepositoryKey(RepositoryNamespace.CLUSTER_RUN_SUMMARY,
                        summaryPartition(ValueUtils.requireNotBlank(jobId, "任务标识"),
                                ValueUtils.requireNotBlank(columnName, "字段名称")),
                        ValueUtils.requireNotBlank(clusterVersion, "聚类版本")),
                ColumnClusteringResult.class);
        return record.isPresent() ? Optional.of(record.get().getPayload())
                : Optional.<ColumnClusteringResult>empty();
    }

    @Override
    public List<ClusterAssignment> findAssignments(String jobId,
                                                   String columnName,
                                                   String clusterVersion) {
        List<RepositoryRecord<ClusterAssignment>> records = repository.findByPartition(
                RepositoryNamespace.CLUSTER_ASSIGNMENT,
                assignmentPartition(ValueUtils.requireNotBlank(jobId, "任务标识"),
                        ValueUtils.requireNotBlank(columnName, "字段名称"),
                        ValueUtils.requireNotBlank(clusterVersion, "聚类版本")),
                ClusterAssignment.class);
        List<ClusterAssignment> assignments = new ArrayList<ClusterAssignment>(records.size());
        for (RepositoryRecord<ClusterAssignment> record : records) {
            assignments.add(record.getPayload());
        }
        return Collections.unmodifiableList(assignments);
    }

    private static String summaryPartition(String jobId, String columnName) {
        return encode(jobId) + encode(columnName);
    }

    private static String assignmentPartition(String jobId,
                                              String columnName,
                                              String clusterVersion) {
        return encode(jobId) + encode(columnName) + encode(clusterVersion);
    }

    private static String encode(String value) {
        return value.length() + ":" + value;
    }
}
