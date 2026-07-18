package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.label.propagation.ClusterPropagationSummary;
import com.fiberhome.ml.raha.repository.adapter.StoredCellLabel;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import java.util.List;

/**
 * 保存直接标签、传播标签和聚类传播摘要。
 */
public interface CellLabelRepository {

    void saveLabels(String jobId,
                    List<CellLabel> labels,
                    ArtifactVersion version,
                    long updatedAt);

    void savePropagationSummaries(String jobId,
                                  List<ClusterPropagationSummary> summaries,
                                  ArtifactVersion version,
                                  long updatedAt);

    void savePropagationResult(String jobId,
                               List<CellLabel> labels,
                               List<ClusterPropagationSummary> summaries,
                               ArtifactVersion version,
                               long updatedAt);

    List<StoredCellLabel> findByJob(String jobId);

    List<StoredCellLabel> findByCell(String jobId, String cellId);

    List<ClusterPropagationSummary> findPropagationSummaries(String jobId);
}
