package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbDetectionWriteContext;
import com.fiberhome.ml.raha.repository.adapter.fmdb.result.FmdbResultWriter;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonValue;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.port.DetectionResultSaveContext;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用最终错误结果表保存疑似错误单元格，并支持跨进程查询恢复。
 */
public final class FmdbDetectionResultRepository
        implements DetectionResultRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbDetectionResultRepository.class);
    /** 最终结果写入器。 */
    private final FmdbResultWriter resultWriter;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 检测结果表名。 */
    private final String tableName;
    /** 模型产物表名。 */
    private final String modelTable;

    public FmdbDetectionResultRepository(
            FmdbResultWriter resultWriter,
            FmdbTableGateway tableGateway,
            FmdbPersistenceConfig persistenceConfig) {
        if (resultWriter == null || tableGateway == null
                || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 检测结果仓储依赖不能为空");
        }
        this.resultWriter = resultWriter;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = FmdbPhysicalTable.DETECTION_RESULT.getTableName();
        this.modelTable = FmdbPhysicalTable.MODEL_ARTIFACT.getTableName();
    }

    @Override
    public void saveAll(String jobId,
                        List<DetectionResult> results,
                        ArtifactVersion version,
                        long updatedAt) {
        throw new IllegalStateException("FMDB 检测结果必须提供可信原始数据集上下文");
    }

    @Override
    public void saveAll(DetectionResultSaveContext context,
                        List<DetectionResult> results,
                        ArtifactVersion version,
                        long updatedAt) {
        if (context == null || results == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("检测上下文、结果、版本和更新时间必须有效");
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.DETECTION_RESULT)) {
            throw new IllegalStateException("FMDB 检测结果持久化已关闭");
        }
        for (DetectionResult result : results) {
            if (result == null || !context.getJobId().equals(result.getJobId())
                    || !context.getDataset().getDatasetId().equals(
                    result.getCoordinate().getDatasetId())) {
                throw new IllegalArgumentException("检测结果与写入上下文不一致");
            }
        }
        String modelSetVersion = modelSetVersion(
                context.getDataset().getDatasetId(), context.getModelVersions());
        FmdbDetectionWriteContext writeContext = new FmdbDetectionWriteContext(
                context.getJobId(), context.getDataset().getTableName(),
                modelSetVersion, trustedRows(context.getDataset()));
        LOGGER.info("开始通过仓储写入 FMDB 检测错误，jobId={}，resultCount={}",
                context.getJobId(), results.size());
        try {
            resultWriter.writeDetectionResults(tableName, writeContext, results);
        } catch (RuntimeException exception) {
            LOGGER.error("FMDB 检测错误仓储写入失败，jobId={}，resultCount={}",
                    context.getJobId(), results.size(), exception);
            throw exception;
        }
    }

    @Override
    public List<DetectionResult> findByJob(String jobId) {
        String validated = ValueUtils.requireNotBlank(jobId, "任务标识");
        List<DetectionResult> results = rows(
                functions.col("detection_batch_id").equalTo(validated));
        Collections.sort(results, Comparator.comparing(
                result -> result.getCoordinate().toCellId()));
        return Collections.unmodifiableList(results);
    }

    @Override
    public Optional<DetectionResult> find(String jobId, String cellId) {
        String validatedJob = ValueUtils.requireNotBlank(jobId, "任务标识");
        String validatedCell = ValueUtils.requireNotBlank(cellId, "单元格标识");
        List<DetectionResult> results = rows(
                functions.col("detection_batch_id").equalTo(validatedJob)
                        .and(functions.col("cell_id").equalTo(validatedCell)));
        if (results.size() > 1) {
            throw new IllegalStateException("同一检测批次和单元格存在多个错误结果");
        }
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    private List<DetectionResult> rows(Column condition) {
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.DETECTION_RESULT)
                || !tableGateway.tableExists(tableName)) {
            return Collections.emptyList();
        }
        LOGGER.debug("开始从 FMDB 查询检测错误，tableName={}", tableName);
        List<Row> rows = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.DETECTION_RESULT),
                condition).collectAsList();
        List<DetectionResult> results = new ArrayList<DetectionResult>(rows.size());
        for (Row row : rows) {
            results.add(toDetectionResult(row));
        }
        return results;
    }

    private static DetectionResult toDetectionResult(Row row) {
        Map<String, Object> reason = FmdbJsonCodec.readObject(
                (String) row.getAs("error_reason_json"));
        CellCoordinate coordinate = new CellCoordinate(
                (String) row.getAs("dataset_id"),
                FmdbJsonValue.requiredText(reason, "snapshotId"),
                (String) row.getAs("row_id"),
                (String) row.getAs("column_name"));
        return new DetectionResult((String) row.getAs("detection_batch_id"),
                FmdbJsonValue.requiredText(reason, "configVersion"),
                FmdbJsonValue.requiredText(reason, "stageId"), coordinate,
                FmdbJsonValue.requiredText(reason, "valueHash"),
                FmdbJsonValue.optionalText(reason, "maskedValue"), true,
                ((Number) row.getAs("score")).doubleValue(),
                ((Number) row.getAs("threshold")).doubleValue(),
                FmdbJsonValue.stringList(reason, "strategyIds"),
                stringMap(reason, "reasons"),
                FmdbJsonValue.requiredText(reason, "modelName"),
                (String) row.getAs("model_version"),
                FmdbJsonValue.requiredText(reason, "featureDictionaryVersion"),
                ((Number) row.getAs("detected_at")).longValue());
    }

    private String modelSetVersion(String datasetId, List<String> modelVersions) {
        if (!tableGateway.tableExists(modelTable)) {
            throw new IllegalStateException("检测结果缺少模型产物表");
        }
        List<Row> rows = tableGateway.read(modelTable,
                Arrays.asList("model_set_version", "model_version"),
                functions.col("dataset_id").equalTo(datasetId)
                        .and(functions.col("model_version").isin(
                                modelVersions.toArray(new Object[0]))))
                .collectAsList();
        Set<String> versions = new LinkedHashSet<String>();
        Set<String> foundModels = new LinkedHashSet<String>();
        for (Row row : rows) {
            versions.add((String) row.getAs("model_set_version"));
            foundModels.add((String) row.getAs("model_version"));
        }
        Set<String> requestedModels = new LinkedHashSet<String>(modelVersions);
        if (!foundModels.equals(requestedModels) || versions.size() != 1) {
            throw new IllegalStateException("检测列模型不属于唯一模型集合：" + versions);
        }
        return versions.iterator().next();
    }

    private static Map<String, Map<String, Object>> trustedRows(RahaDataset dataset) {
        Map<String, Map<String, Object>> rows =
                new LinkedHashMap<String, Map<String, Object>>();
        for (Row row : dataset.getDataFrame().collectAsList()) {
            String rowId = String.valueOf((Object) row.getAs(dataset.getRowIdColumn()));
            Map<String, Object> values = new LinkedHashMap<String, Object>();
            for (StructField field : dataset.getDataFrame().schema().fields()) {
                if (!RowIdentityColumns.isTechnical(field.name())) {
                    values.put(field.name(), row.getAs(field.name()));
                }
            }
            if (rows.put(rowId, values) != null) {
                throw new IllegalStateException("检测输入包含重复逻辑行：" + rowId);
            }
        }
        return rows;
    }

    private static Map<String, String> stringMap(Map<String, Object> root,
                                                 String key) {
        Object raw = root.get(key);
        if (raw == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> values = FmdbJsonValue.objectMap(raw, key);
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (!(entry.getValue() instanceof String)) {
                throw new IllegalArgumentException("检测原因必须使用文本值");
            }
            result.put(entry.getKey(), (String) entry.getValue());
        }
        return result;
    }
}
