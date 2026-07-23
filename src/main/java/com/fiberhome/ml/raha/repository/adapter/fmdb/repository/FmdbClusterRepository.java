package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.cluster.domain.ClusterAssignment;
import com.fiberhome.ml.raha.cluster.domain.ColumnClusteringResult;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbClusterSummaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.ClusterRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Iterator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 暂存当前任务聚类结果，并从列级摘要和训练单元格恢复聚类状态。
 */
public final class FmdbClusterRepository implements ClusterRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbClusterRepository.class);
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 当前任务尚未统一物化的聚类结果。 */
    private final Map<String, ColumnClusteringResult> pendingResults =
            new LinkedHashMap<String, ColumnClusteringResult>();
    /** 训练列级产物表名。 */
    private final String columnArtifactTable;
    /** 训练单元格表名。 */
    private final String trainingCellTable;

    public FmdbClusterRepository(FmdbTableGateway tableGateway,
                                 FmdbPersistenceConfig persistenceConfig) {
        if (tableGateway == null || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 聚类仓储依赖不能为空");
        }
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.columnArtifactTable = FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT
                .getTableName();
        this.trainingCellTable = FmdbPhysicalTable.TRAINING_CELL.getTableName();
    }

    @Override
    public synchronized void save(String jobId,
                                  ColumnClusteringResult result,
                                  ArtifactVersion version,
                                  long updatedAt) {
        if (result == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("聚类结果、版本和更新时间必须有效");
        }
        pendingResults.put(key(jobId, result.getColumnName(),
                result.getClusterVersion()), result);
        LOGGER.debug("聚类结果已进入统一物化缓冲，jobId={}，columnName={}，assignmentCount={}",
                jobId, result.getColumnName(), result.getAssignments().size());
    }

    @Override
    public synchronized Optional<ColumnClusteringResult> findResult(
            String jobId, String columnName, String clusterVersion) {
        String key = key(jobId, columnName, clusterVersion);
        ColumnClusteringResult pending = pendingResults.get(key);
        if (pending != null) {
            return Optional.of(pending);
        }
        LOGGER.debug("训练聚类结果仅使用当前任务缓存，未命中缓存，jobId={}，columnName={}，clusterVersion={}",
                jobId, columnName, clusterVersion);
        return Optional.empty();
    }

    @Override
    public synchronized List<ClusterAssignment> findAssignments(
            String jobId, String columnName, String clusterVersion) {
        ColumnClusteringResult pending = pendingResults.get(
                key(jobId, columnName, clusterVersion));
        if (pending != null) {
            return pending.getAssignments();
        }
        LOGGER.debug("训练聚类成员仅使用当前任务缓存，未命中缓存，jobId={}，columnName={}，clusterVersion={}",
                jobId, columnName, clusterVersion);
        return Collections.emptyList();
    }

    @Override
    public synchronized void release(String jobId, Set<String> columns) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (columns == null) {
            throw new IllegalArgumentException("待释放聚类字段不能为空");
        }
        Iterator<Map.Entry<String, ColumnClusteringResult>> iterator =
                pendingResults.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, ColumnClusteringResult> entry = iterator.next();
            if (entry.getKey().startsWith(validatedJobId.length() + ":"
                    + validatedJobId)
                    && columns.contains(entry.getValue().getColumnName())) {
                iterator.remove();
            }
        }
        LOGGER.debug("任务级列聚类缓存已释放，jobId={}，columns={}",
                validatedJobId, columns);
    }

    private static String key(String jobId, String columnName, String clusterVersion) {
        String job = ValueUtils.requireNotBlank(jobId, "任务标识");
        String column = ValueUtils.requireNotBlank(columnName, "字段名称");
        String version = ValueUtils.requireNotBlank(clusterVersion, "聚类版本");
        return job.length() + ":" + job + column.length() + ":" + column
                + version.length() + ":" + version;
    }
}
