package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.cluster.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.ColumnClusteringResult;

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
