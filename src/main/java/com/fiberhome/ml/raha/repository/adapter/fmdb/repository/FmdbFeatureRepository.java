package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbFeatureDictionaryCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.FeatureRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 暂存当前任务特征，并从列级产物和训练单元格表恢复字典及向量。
 */
public final class FmdbFeatureRepository implements FeatureRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbFeatureRepository.class);
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 当前任务尚未统一物化的特征字典。 */
    private final Map<String, FeatureDictionary> pendingDictionaries =
            new LinkedHashMap<String, FeatureDictionary>();
    /** 当前任务尚未统一物化的特征向量。 */
    private final Map<String, List<SparseFeatureRow>> pendingRows =
            new LinkedHashMap<String, List<SparseFeatureRow>>();
    /** 训练列级产物表名。 */
    private final String columnArtifactTable;
    /** 训练单元格表名。 */
    private final String trainingCellTable;

    public FmdbFeatureRepository(FmdbTableGateway tableGateway,
                                 FmdbPersistenceConfig persistenceConfig) {
        if (tableGateway == null || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 特征仓储依赖不能为空");
        }
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.columnArtifactTable = FmdbPhysicalTable.TRAINING_COLUMN_ARTIFACT
                .getTableName();
        this.trainingCellTable = FmdbPhysicalTable.TRAINING_CELL.getTableName();
    }

    @Override
    public synchronized void save(String jobId,
                                  FeatureAssemblyResult result,
                                  ArtifactVersion version,
                                  long updatedAt) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (result == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("特征结果、版本和更新时间必须有效");
        }
        for (Map.Entry<String, FeatureDictionary> entry
                : result.getDictionaries().entrySet()) {
            pendingDictionaries.put(key(validatedJobId, entry.getKey()),
                    entry.getValue());
        }
        Map<String, List<SparseFeatureRow>> grouped =
                new LinkedHashMap<String, List<SparseFeatureRow>>();
        for (SparseFeatureRow row : result.getRows()) {
            if (!grouped.containsKey(row.getColumnName())) {
                grouped.put(row.getColumnName(), new ArrayList<SparseFeatureRow>());
            }
            grouped.get(row.getColumnName()).add(row);
        }
        for (Map.Entry<String, List<SparseFeatureRow>> entry : grouped.entrySet()) {
            pendingRows.put(key(validatedJobId, entry.getKey()),
                    Collections.unmodifiableList(
                            new ArrayList<SparseFeatureRow>(entry.getValue())));
        }
        LOGGER.info("特征阶段保存完成，jobId={}，dictionaryCount={}，rowCount={}",
                validatedJobId, result.getDictionaries().size(), result.getRows().size());
    }

    @Override
    public synchronized Optional<FeatureDictionary> findDictionary(
            String jobId, String columnName) {
        String key = key(jobId, columnName);
        FeatureDictionary pending = pendingDictionaries.get(key);
        if (pending != null) {
            return Optional.of(pending);
        }
        LOGGER.debug("训练特征字典仅使用当前任务缓存，未命中缓存，jobId={}，columnName={}",
                jobId, columnName);
        return Optional.empty();
    }

    @Override
    public synchronized List<SparseFeatureRow> findRows(String jobId,
                                                        String columnName) {
        String key = key(jobId, columnName);
        List<SparseFeatureRow> pending = pendingRows.get(key);
        if (pending != null) {
            return pending;
        }
        LOGGER.debug("训练特征向量仅使用当前任务缓存，未命中缓存，jobId={}，columnName={}",
                jobId, columnName);
        return Collections.emptyList();
    }

    @Override
    public synchronized void release(String jobId, Set<String> columns) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        if (columns == null) {
            throw new IllegalArgumentException("待释放特征字段不能为空");
        }
        for (String column : columns) {
            String cacheKey = key(validatedJobId, column);
            pendingDictionaries.remove(cacheKey);
            pendingRows.remove(cacheKey);
        }
        LOGGER.debug("任务级列特征缓存已释放，jobId={}，columns={}",
                validatedJobId, columns);
    }

    private static Map<Integer, Double> featureValues(String json) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        for (Map.Entry<String, Object> entry : FmdbJsonCodec.readObject(json).entrySet()) {
            if (!(entry.getValue() instanceof Number)) {
                throw new IllegalArgumentException("特征向量 JSON 包含非数值字段");
            }
            values.put(Integer.valueOf(entry.getKey()),
                    ((Number) entry.getValue()).doubleValue());
        }
        return values;
    }

    private static Map<String, String> stringValues(String json) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Object> entry : FmdbJsonCodec.readObject(json).entrySet()) {
            if (!(entry.getValue() instanceof String)) {
                throw new IllegalArgumentException("特征摘要 JSON 包含非文本字段");
            }
            values.put(entry.getKey(), (String) entry.getValue());
        }
        return values;
    }

    private static String key(String jobId, String columnName) {
        String job = ValueUtils.requireNotBlank(jobId, "任务标识");
        String column = ValueUtils.requireNotBlank(columnName, "字段名称");
        return job.length() + ":" + job + column.length() + ":" + column;
    }
}
