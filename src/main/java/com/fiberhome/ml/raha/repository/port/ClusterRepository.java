package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import java.util.List;
import java.util.Optional;

/**
 * 保存和读取列聚类运行结果及成员映射。
 */
public interface ClusterRepository {

    void save(String jobId,
              ColumnClusteringResult result,
              ArtifactVersion version,
              long updatedAt);

    Optional<ColumnClusteringResult> findResult(String jobId,
                                                String columnName,
                                                String clusterVersion);

    List<ClusterAssignment> findAssignments(String jobId,
                                            String columnName,
                                            String clusterVersion);
}
