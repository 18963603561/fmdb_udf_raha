package com.fiberhome.ml.raha.repository.adapter.fmdb.repository;

import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.data.loader.identity.RowFingerprintAlgorithm;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityMode;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.repository.adapter.fmdb.gateway.FmdbTableGateway;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPhysicalTable;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableRecord;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbTableSchemas;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.ModelMetadataRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于 `raha_model_artifact` 追加状态快照的模型元数据仓储。
 * 模型参数由 FmdbModelStore 先写入，本仓储只复用其不可变载荷并追加状态。
 */
public final class FmdbModelMetadataRepository implements ModelMetadataRepository {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbModelMetadataRepository.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** FMDB 持久化开关。 */
    private final FmdbPersistenceConfig persistenceConfig;
    /** 模型物理表名。 */
    private final String tableName;

    public FmdbModelMetadataRepository(SparkSession sparkSession,
                                       FmdbTableGateway tableGateway,
                                       FmdbPersistenceConfig persistenceConfig) {
        if (sparkSession == null || tableGateway == null || persistenceConfig == null) {
            throw new IllegalArgumentException("FMDB 模型元数据仓储依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.persistenceConfig = persistenceConfig;
        this.tableName = FmdbPhysicalTable.MODEL_ARTIFACT.getTableName();
    }

    @Override
    public synchronized void saveAll(List<RahaColumnModel> models,
                                     ArtifactVersion version,
                                     long updatedAt) {
        if (models == null || version == null || updatedAt <= 0L) {
            throw new IllegalArgumentException("模型元数据、版本和更新时间必须有效");
        }
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.MODEL_ARTIFACT)) {
            throw new IllegalStateException("FMDB 模型元数据持久化已关闭");
        }
        for (RahaColumnModel model : models) {
            if (model == null) {
                throw new IllegalArgumentException("模型元数据不能包含空值");
            }
            Row base = baseRow(model);
            if (base == null) {
                throw new IllegalStateException("模型参数行不存在，不能追加状态："
                        + model.getModelVersion());
            }
            Map<String, Object> values = new LinkedHashMap<String, Object>();
            copyBase(values, base);
            if (sameState(base, model)) {
                LOGGER.info("FMDB 模型状态未变化，跳过重复快照，modelVersion={}，status={}",
                        model.getModelVersion(), model.getStatus());
                continue;
            }
            values.put("model_set_status", model.getStatus().name());
            values.put("state_version", Math.addExact(
                    ((Number) base.getAs("state_version")).intValue(), 1));
            values.put("strategy_plan_version", model.getStrategyPlanVersion());
            values.put("threshold", model.getThreshold());
            values.put("metrics_json", FmdbJsonCodec.write(model.getMetrics()));
            values.put("published_at", model.getPublishedAt());
            Dataset<Row> frame = sparkSession.createDataFrame(
                    Collections.singletonList(FmdbTableRecord.of(
                            FmdbPhysicalTable.MODEL_ARTIFACT, values).toRow()),
                    FmdbTableSchemas.schema(FmdbPhysicalTable.MODEL_ARTIFACT));
            long written = tableGateway.append(tableName, frame,
                    Arrays.asList("model_set_version", "column_name",
                            "model_version", "state_version"), 1L);
            LOGGER.info("FMDB 模型状态追加完成，datasetId={}，columnName={}，"
                            + "modelVersion={}，status={}，writtenCount={}",
                    model.getDatasetId(), model.getColumnName(), model.getModelVersion(),
                    model.getStatus(), written);
        }
    }

    @Override
    public Optional<RahaColumnModel> find(String datasetId, String columnName,
                                          String modelVersion) {
        List<Row> rows = rows(datasetId, columnName,
                functions.col("model_version").equalTo(
                        ValueUtils.requireNotBlank(modelVersion, "模型版本")));
        return rows.isEmpty() ? Optional.<RahaColumnModel>empty()
                : Optional.of(toModel(latestState(rows)));
    }

    @Override
    public List<RahaColumnModel> findByColumn(String datasetId, String columnName) {
        List<Row> rows = rows(datasetId, columnName, null);
        Map<String, Row> latestByVersion = new LinkedHashMap<String, Row>();
        for (Row row : rows) {
            String version = (String) row.getAs("model_version");
            Row current = latestByVersion.get(version);
            if (current == null || ((Number) row.getAs("state_version")).intValue()
                    > ((Number) current.getAs("state_version")).intValue()) {
                latestByVersion.put(version, row);
            }
        }
        List<RahaColumnModel> models = new ArrayList<RahaColumnModel>();
        for (Row row : latestByVersion.values()) {
            models.add(toModel(row));
        }
        Collections.sort(models, Comparator.comparingLong(RahaColumnModel::getCreatedAt)
                .thenComparing(RahaColumnModel::getModelVersion)
                .thenComparing(model -> stateRank(model.getStatus())));
        return Collections.unmodifiableList(models);
    }

    @Override
    public List<RahaColumnModel> findByModelSetVersion(String modelSetVersion) {
        String version = ValueUtils.requireNotBlank(
                modelSetVersion, "模型集合版本");
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.MODEL_ARTIFACT)
                || !tableGateway.tableExists(tableName)) {
            return Collections.emptyList();
        }
        LOGGER.debug("开始按模型集合版本读取 FMDB 列模型，modelSetVersion={}",
                version);
        List<Row> rows = tableGateway.read(tableName,
                FmdbTableSchemas.columns(FmdbPhysicalTable.MODEL_ARTIFACT),
                functions.col("model_set_version").equalTo(version))
                .collectAsList();
        Map<String, Row> latestByModel = new LinkedHashMap<String, Row>();
        for (Row row : rows) {
            String key = String.valueOf((Object) row.getAs("column_name"))
                    + "\u0000" + String.valueOf((Object) row.getAs("model_version"));
            Row current = latestByModel.get(key);
            if (current == null || ((Number) row.getAs("state_version")).intValue()
                    > ((Number) current.getAs("state_version")).intValue()) {
                latestByModel.put(key, row);
            }
        }
        List<RahaColumnModel> models = new ArrayList<RahaColumnModel>();
        for (Row row : latestByModel.values()) {
            models.add(toModel(row));
        }
        Collections.sort(models, Comparator.comparing(
                RahaColumnModel::getColumnName)
                .thenComparing(RahaColumnModel::getModelVersion));
        LOGGER.debug("FMDB 模型集合读取完成，modelSetVersion={}，modelCount={}",
                version, models.size());
        return Collections.unmodifiableList(models);
    }

    @Override
    public Optional<RahaColumnModel> findPublished(String datasetId,
                                                   String columnName) {
        List<RahaColumnModel> published = new ArrayList<RahaColumnModel>();
        for (RahaColumnModel model : findByColumn(datasetId, columnName)) {
            if (model.getStatus() == ModelStatus.PUBLISHED) {
                published.add(model);
            }
        }
        if (published.size() > 1) {
            throw new IllegalStateException("同一字段存在多个已发布模型");
        }
        return published.isEmpty() ? Optional.<RahaColumnModel>empty()
                : Optional.of(published.get(0));
    }

    private List<Row> rows(String datasetId, String columnName,
                           org.apache.spark.sql.Column extraCondition) {
        if (!persistenceConfig.shouldPersist(FmdbPhysicalTable.MODEL_ARTIFACT)
                || !tableGateway.tableExists(tableName)) {
            return Collections.emptyList();
        }
        org.apache.spark.sql.Column condition = functions.col("dataset_id")
                .equalTo(ValueUtils.requireNotBlank(datasetId, "数据集标识"))
                .and(functions.col("column_name")
                        .equalTo(ValueUtils.requireNotBlank(columnName, "字段名称")));
        if (extraCondition != null) {
            condition = condition.and(extraCondition);
        }
        return tableGateway.read(tableName,
                        FmdbTableSchemas.columns(FmdbPhysicalTable.MODEL_ARTIFACT),
                        condition)
                .collectAsList();
    }

    private Row baseRow(RahaColumnModel model) {
        List<Row> rows = rows(model.getDatasetId(), model.getColumnName(),
                functions.col("model_version").equalTo(model.getModelVersion()));
        return rows.isEmpty() ? null : latestState(rows);
    }

    private static Row latestState(List<Row> rows) {
        Row latest = rows.get(0);
        for (int index = 1; index < rows.size(); index++) {
            Row candidate = rows.get(index);
            if (((Number) candidate.getAs("state_version")).intValue()
                    > ((Number) latest.getAs("state_version")).intValue()) {
                latest = candidate;
            }
        }
        return latest;
    }

    private static int stateRank(ModelStatus status) {
        switch (status) {
            case DISABLED: return 5;
            case PUBLISHED: return 4;
            case CANDIDATE: return 3;
            case DRAFT: return 2;
            case FAILED: return 1;
            default: return 0;
        }
    }

    private static boolean sameState(Row row, RahaColumnModel model) {
        Object publishedAt = row.getAs("published_at");
        return model.getStatus().name().equals(row.getAs("model_set_status"))
                && Double.compare(model.getThreshold(),
                ((Number) row.getAs("threshold")).doubleValue()) == 0
                && FmdbJsonCodec.write(model.getMetrics()).equals(
                row.getAs("metrics_json"))
                && (model.getPublishedAt() == null
                ? publishedAt == null
                : publishedAt != null && model.getPublishedAt().equals(
                        ((Number) publishedAt).longValue()));
    }

    private static void copyBase(Map<String, Object> target, Row base) {
        String[] columns = FmdbTableSchemas.schema(FmdbPhysicalTable.MODEL_ARTIFACT)
                .fieldNames();
        for (String column : columns) {
            target.put(column, base.getAs(column));
        }
    }

    private static RahaColumnModel toModel(Row row) {
        Map<String, Object> payload = FmdbJsonCodec.readObject(
                (String) row.getAs("model_payload_json"));
        Map<String, Double> metrics = metrics((String) row.getAs("metrics_json"));
        Object published = row.getAs("published_at");
        return new RahaColumnModel((String) payload.get("modelName"),
                (String) row.getAs("model_version"), (String) row.getAs("dataset_id"),
                (String) row.getAs("column_name"),
                schemaHash(row), ClassifierType.valueOf(
                        (String) row.getAs("classifier_type")),
                (String) row.getAs("feature_dictionary_version"),
                (String) row.getAs("strategy_plan_version"),
                ((Number) row.getAs("threshold")).doubleValue(),
                (String) row.getAs("model_path"),
                ModelStatus.valueOf((String) row.getAs("model_set_status")), metrics,
                ((Number) row.getAs("created_at")).longValue(),
                published == null ? null : ((Number) published).longValue(),
                (String) row.getAs("model_set_version"), rowIdentity(payload));
    }

    private static RowIdentityConfig rowIdentity(Map<String, Object> payload) {
        Object rawMode = payload.get("rowIdentityMode");
        Object rawAlgorithm = payload.get("rowFingerprintAlgorithm");
        Object rawVersion = payload.get("rowFingerprintVersion");
        if (!(rawMode instanceof String) || !(rawAlgorithm instanceof String)
                || !(rawVersion instanceof String)) {
            // 兼容改造前的模型载荷；新模型必须显式保存完整行身份规则。
            return RowIdentityConfig.contentHash();
        }
        List<String> keyColumns = new ArrayList<String>();
        Object rawKeys = payload.get("rowKeyColumns");
        if (rawKeys instanceof List) {
            for (Object key : (List<?>) rawKeys) {
                if (!(key instanceof String)) {
                    throw new IllegalStateException("模型行身份业务键必须为文本");
                }
                keyColumns.add((String) key);
            }
        }
        return new RowIdentityConfig(RowIdentityMode.valueOf((String) rawMode),
                keyColumns, RowFingerprintAlgorithm.valueOf(
                (String) rawAlgorithm), (String) rawVersion);
    }

    private static String schemaHash(Row row) {
        Object value = row.getAs("schema_hash");
        return ValueUtils.requireNotBlank((String) value, "模型模式哈希");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Double> metrics(String json) {
        Map<String, Object> raw = FmdbJsonCodec.readObject(json);
        Map<String, Double> result = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            if (!(entry.getValue() instanceof Number)) {
                throw new IllegalStateException("模型指标必须为数值：" + entry.getKey());
            }
            result.put(entry.getKey(), ((Number) entry.getValue()).doubleValue());
        }
        return result;
    }
}
