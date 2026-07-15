package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.ClusterPropagationSummary;
import com.fiberhome.ml.raha.util.ValueUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 基于统一仓储保存标签和传播摘要，并保留业务版本。
 */
public final class DefaultCellLabelRepository implements CellLabelRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultCellLabelRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public void saveLabels(String jobId,
                           List<CellLabel> labels,
                           ArtifactVersion version,
                           long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (labels == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("标签集合、版本和更新时间必须有效");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (CellLabel label : labels) {
                if (label == null) {
                    throw new IllegalArgumentException("标签集合不能包含空值");
                }
                transactionRepository.save(new RepositoryRecord<CellLabel>(
                        new RepositoryKey(RepositoryNamespace.CELL_LABEL,
                                validatedJobId, label.getLabelId()),
                        version, label, updatedAt));
            }
        });
    }

    @Override
    public void savePropagationSummaries(String jobId,
                                         List<ClusterPropagationSummary> summaries,
                                         ArtifactVersion version,
                                         long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (summaries == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("传播摘要、版本和更新时间必须有效");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (ClusterPropagationSummary summary : summaries) {
                if (summary == null) {
                    throw new IllegalArgumentException("传播摘要不能包含空值");
                }
                transactionRepository.save(new RepositoryRecord<ClusterPropagationSummary>(
                        new RepositoryKey(RepositoryNamespace.LABEL_PROPAGATION_SUMMARY,
                                validatedJobId, summary.getSummaryId()),
                        version, summary, updatedAt));
            }
        });
    }

    @Override
    public void savePropagationResult(String jobId,
                                      List<CellLabel> labels,
                                      List<ClusterPropagationSummary> summaries,
                                      ArtifactVersion version,
                                      long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (labels == null || summaries == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("传播结果、版本和更新时间必须有效");
        }
        repository.executeInTransaction(transactionRepository -> {
            for (CellLabel label : labels) {
                if (label == null) {
                    throw new IllegalArgumentException("传播标签集合不能包含空值");
                }
                transactionRepository.save(new RepositoryRecord<CellLabel>(
                        new RepositoryKey(RepositoryNamespace.CELL_LABEL,
                                validatedJobId, label.getLabelId()),
                        version, label, updatedAt));
            }
            for (ClusterPropagationSummary summary : summaries) {
                if (summary == null) {
                    throw new IllegalArgumentException("传播摘要不能包含空值");
                }
                transactionRepository.save(new RepositoryRecord<ClusterPropagationSummary>(
                        new RepositoryKey(RepositoryNamespace.LABEL_PROPAGATION_SUMMARY,
                                validatedJobId, summary.getSummaryId()),
                        version, summary, updatedAt));
            }
        });
    }

    @Override
    public List<StoredCellLabel> findByJob(String jobId) {
        List<RepositoryRecord<CellLabel>> records = repository.findByPartition(
                RepositoryNamespace.CELL_LABEL,
                ValueUtils.requireNotBlank(jobId, "任务标识"), CellLabel.class);
        List<StoredCellLabel> labels = new ArrayList<StoredCellLabel>(records.size());
        for (RepositoryRecord<CellLabel> record : records) {
            labels.add(new StoredCellLabel(record.getPayload(),
                    record.getVersion(), record.getUpdatedAt()));
        }
        return Collections.unmodifiableList(labels);
    }

    @Override
    public List<StoredCellLabel> findByCell(String jobId, String cellId) {
        String validatedCellId = ValueUtils.requireNotBlank(cellId, "单元格标识");
        List<StoredCellLabel> matches = new ArrayList<StoredCellLabel>();
        for (StoredCellLabel stored : findByJob(jobId)) {
            if (stored.getLabel().getCellId().equals(validatedCellId)) {
                matches.add(stored);
            }
        }
        return Collections.unmodifiableList(matches);
    }

    @Override
    public List<ClusterPropagationSummary> findPropagationSummaries(String jobId) {
        List<RepositoryRecord<ClusterPropagationSummary>> records = repository.findByPartition(
                RepositoryNamespace.LABEL_PROPAGATION_SUMMARY,
                ValueUtils.requireNotBlank(jobId, "任务标识"),
                ClusterPropagationSummary.class);
        List<ClusterPropagationSummary> summaries =
                new ArrayList<ClusterPropagationSummary>(records.size());
        for (RepositoryRecord<ClusterPropagationSummary> record : records) {
            summaries.add(record.getPayload());
        }
        return Collections.unmodifiableList(summaries);
    }
}
