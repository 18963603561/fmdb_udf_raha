package com.fiberhome.ml.raha.detect;

import com.fiberhome.ml.raha.api.DetectRequest;
import com.fiberhome.ml.raha.api.DetectResult;
import com.fiberhome.ml.raha.config.RahaConfig;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.data.TargetColumnResolver;
import com.fiberhome.ml.raha.fmdb.FmdbDatasetLoader;
import com.fiberhome.ml.raha.model.ModelScoreExpression;
import com.fiberhome.ml.raha.model.ModelStore;
import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.model.RahaModelSet;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;
import com.fiberhome.ml.raha.support.TimeUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.sum;

/**
 * 同步检测用例，严格使用模型集合冻结字典和列模型。
 */
public final class RahaDetectService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaDetectService.class);
    /** 数据加载器。 */
    private final FmdbDatasetLoader datasetLoader;
    /** 模型存储。 */
    private final ModelStore modelStore;
    /** 检测存储。 */
    private final DetectionStore detectionStore;
    /** Spark 模型表达式。 */
    private final ModelScoreExpression scoreExpression;
    /** 检测解释器。 */
    private final DetectionExplainer explainer;
    /** 根配置。 */
    private final RahaConfig config;

    public RahaDetectService(FmdbDatasetLoader datasetLoader, ModelStore modelStore,
                             DetectionStore detectionStore,
                             ModelScoreExpression scoreExpression,
                             DetectionExplainer explainer, RahaConfig config) {
        this.datasetLoader = datasetLoader;
        this.modelStore = modelStore;
        this.detectionStore = detectionStore;
        this.scoreExpression = scoreExpression;
        this.explainer = explainer;
        this.config = config;
    }

    public DetectResult detect(DetectRequest request) {
        long startedAt = System.currentTimeMillis();
        if (request.getModelSetVersion() == null
                || request.getModelSetVersion().trim().isEmpty()) {
            throw new RahaException(RahaErrorCode.INVALID_REQUEST, "模型集合版本不能为空");
        }
        RahaModelSet modelSet = modelStore.findModelSet(request.getModelSetVersion())
                .orElseThrow(() -> new RahaException(RahaErrorCode.INVALID_REQUEST,
                        "模型集合不存在：" + request.getModelSetVersion()));
        List<String> targets = TargetColumnResolver.resolve(request.getTargetColumns(),
                modelSet.getModelColumns());
        List<String> rowKeys = request.getRowKeyColumns().isEmpty()
                ? modelSet.getRowKeyColumns() : request.getRowKeyColumns();
        RahaDataset dataset = datasetLoader.load(request.getInputReference(),
                modelSet.getDatasetId(), request.getSourceType(), rowKeys,
                request.getSnapshotId(), targets);
        try {
            String fingerprint = fingerprint(dataset, modelSet, targets,
                    request.isErrorsOnly());
            String batchId = HashUtils.shortId("detect", fingerprint);
            Optional<DetectionBatch> existing = detectionStore.findBatch(batchId);
            if (existing.isPresent()) {
                DetectionBatch batch = existing.get();
                return new DetectResult(batchId, batch.getTargetColumns(),
                        batch.getEvaluatedCellCount(), batch.getDetectedCellCount(),
                        "fmdb://dw.raha_detection_result/" + batchId,
                        System.currentTimeMillis() - startedAt);
            }
            Map<String, RahaColumnModel> modelByColumn = new LinkedHashMap<String, RahaColumnModel>();
            for (RahaColumnModel model : modelStore.loadColumnModels(
                    modelSet.getModelSetVersion())) {
                modelByColumn.put(model.getColumnName(), model);
            }
            long createdAt = System.currentTimeMillis();
            String partitionDate = TimeUtils.partitionDate(createdAt,
                    config.getPartitionTimeZone());
            long evaluated = dataset.getInputRowCount() * targets.size();
            DetectionBatch writingBatch = new DetectionBatch(batchId, fingerprint,
                    dataset.getDatasetId(), dataset.getSnapshotId(),
                    dataset.getInputReference(), dataset.getSourceType(),
                    dataset.getRowIdentityMode(), dataset.getRowKeyColumns(), targets,
                    dataset.getSchemaHash(), modelSet.getModelSetVersion(),
                    request.isErrorsOnly(), dataset.getInputRowCount(), evaluated, 0L,
                    createdAt);
            long detected = 0L;
            for (String column : targets) {
                RahaColumnModel model = modelByColumn.get(column);
                if (model == null) {
                    throw new RahaException(RahaErrorCode.INCOMPATIBLE_MODEL,
                            "模型集合缺少列模型：" + column);
                }
                Column score = scoreExpression.score(model, col(column));
                Dataset<Row> scored = dataset.getRows()
                        .withColumn("score", score)
                        .withColumn("is_error", score.geq(model.getThreshold()))
                        .withColumn("row_id", col(RahaDataset.ROW_ID))
                        .withColumn("duplicate_count", col(RahaDataset.DUPLICATE_COUNT))
                        .withColumn("value_hash", sha2(coalesce(col(column).cast("string"),
                                lit("")), 256))
                        .withColumn("reason_json", lit(explainer.explain(model)))
                        .select("row_id", "duplicate_count", "value_hash", "is_error",
                                "score", "reason_json");
                Dataset<Row> output = request.isErrorsOnly()
                        ? scored.filter(col("is_error").equalTo(true)) : scored;
                Row detectedRow = scored.filter(col("is_error").equalTo(true))
                        .agg(sum(col("duplicate_count")).alias("detected")).first();
                Object detectedValue = detectedRow.getAs("detected");
                detected += detectedValue == null ? 0L : ((Number) detectedValue).longValue();
                detectionStore.appendResults(writingBatch, model, output, partitionDate);
                LOGGER.info("字段检测完成，column={}，modelVersion={}，threshold={}，"
                                + "logicalErrors={}", column, model.getModelVersion(),
                        model.getThreshold(), detectedValue == null ? 0L : detectedValue);
            }
            DetectionBatch committed = new DetectionBatch(batchId, fingerprint,
                    dataset.getDatasetId(), dataset.getSnapshotId(),
                    dataset.getInputReference(), dataset.getSourceType(),
                    dataset.getRowIdentityMode(), dataset.getRowKeyColumns(), targets,
                    dataset.getSchemaHash(), modelSet.getModelSetVersion(),
                    request.isErrorsOnly(), dataset.getInputRowCount(), evaluated, detected,
                    createdAt);
            detectionStore.saveBatch(committed);
            LOGGER.info("检测完成，detectionBatchId={}，modelSetVersion={}，"
                            + "targetColumns={}，evaluatedCells={}，detectedCells={}，"
                            + "elapsedMillis={}", batchId, modelSet.getModelSetVersion(),
                    targets, evaluated, detected, System.currentTimeMillis() - startedAt);
            return new DetectResult(batchId, targets, evaluated, detected,
                    "fmdb://dw.raha_detection_result/" + batchId,
                    System.currentTimeMillis() - startedAt);
        } catch (RuntimeException exception) {
            LOGGER.error("检测失败，modelSetVersion={}，datasetId={}，targetColumns={}",
                    request.getModelSetVersion(), dataset.getDatasetId(), targets, exception);
            throw exception;
        } finally {
            dataset.getRows().unpersist(false);
        }
    }

    private static String fingerprint(RahaDataset dataset, RahaModelSet modelSet,
                                      List<String> targets, boolean errorsOnly) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put("datasetId", dataset.getDatasetId());
        values.put("snapshotId", dataset.getSnapshotId());
        values.put("inputReference", dataset.getInputReference());
        values.put("rowIdentityMode", dataset.getRowIdentityMode().name());
        values.put("rowKeyColumns", dataset.getRowKeyColumns());
        values.put("targetColumns", targets);
        values.put("modelSetVersion", modelSet.getModelSetVersion());
        values.put("errorsOnly", errorsOnly);
        return HashUtils.sha256(JsonUtils.toJson(values));
    }
}
