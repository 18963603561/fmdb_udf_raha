package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.config.RahaConfig;
import com.fiberhome.ml.raha.data.CellLabel;
import com.fiberhome.ml.raha.fmdb.FmdbLabelStore;
import com.fiberhome.ml.raha.fmdb.FmdbSampleStore;
import com.fiberhome.ml.raha.fmdb.FmdbTableGateway;
import com.fiberhome.ml.raha.fmdb.RahaTables;
import com.fiberhome.ml.raha.sample.SampleBatch;
import com.fiberhome.ml.raha.sample.SampleTuple;
import com.fiberhome.ml.raha.support.FormCodec;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.TimeUtils;
import com.fiberhome.ml.raha.support.ValueNormalizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.col;

/**
 * 在 Spark 驱动进程中实测采样、训练、检测三个 UDF，并使用干净数据评估。
 */
public final class RahaToyUdfValidationMain {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(RahaToyUdfValidationMain.class);

    private RahaToyUdfValidationMain() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            throw new IllegalArgumentException("参数必须为 dirty.csv clean.csv report.json");
        }
        SparkSession spark = SparkSession.builder()
                .appName("raha-toy-udf-validation")
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        long startedAt = System.currentTimeMillis();
        String dirtyPath = args[0];
        String cleanPath = args[1];
        String reportPath = args[2];
        String datasetId = "toy_validation_" + startedAt;
        LOGGER.info("开始 toy UDF 全链路验证，datasetId={}，dirtyPath={}，cleanPath={}",
                datasetId, dirtyPath, cleanPath);
        String sampleRequest = "inputReference=" + FormCodec.encode("csv:" + dirtyPath)
                + "&datasetId=" + FormCodec.encode(datasetId)
                + "&rowKeyColumns=ID&labelingBudget=20";
        String sampleJson = new F_DW_RAHASAMPLE().evaluate(sampleRequest);
        String sampleBatchId = requiredResult(sampleJson, "sampleBatchId");
        FmdbTableGateway gateway = new FmdbTableGateway(spark, RahaConfig.defaults());
        FmdbSampleStore sampleStore = new FmdbSampleStore(gateway);
        FmdbLabelStore labelStore = new FmdbLabelStore(gateway);
        SampleBatch batch = sampleStore.findBatch(sampleBatchId)
                .orElseThrow(() -> new IllegalStateException("采样批次未提交"));
        List<SampleTuple> tuples = sampleStore.loadTuples(
                java.util.Collections.singletonList(sampleBatchId));
        Map<String, Map<String, String>> cleanRows = rowsById(spark, cleanPath);
        List<CellLabel> labels = createLabels(batch, tuples, cleanRows);
        labelStore.save(labels);
        String trainJson = new F_DW_RAHATRAIN().evaluate(
                "sampleBatchIds=" + FormCodec.encode(sampleBatchId));
        String fullModelSetVersion = requiredResult(trainJson, "modelSetVersion");
        String incrementalSampleJson = new F_DW_RAHASAMPLE().evaluate(sampleRequest
                + "&targetColumns=Kingdom");
        String incrementalSampleBatchId = requiredResult(incrementalSampleJson,
                "sampleBatchId");
        SampleBatch incrementalBatch = sampleStore.findBatch(incrementalSampleBatchId)
                .orElseThrow(() -> new IllegalStateException("增量采样批次未提交"));
        List<SampleTuple> incrementalTuples = sampleStore.loadTuples(
                java.util.Collections.singletonList(incrementalSampleBatchId));
        labelStore.save(createLabels(incrementalBatch, incrementalTuples, cleanRows));
        String incrementalTrainJson = new F_DW_RAHATRAIN().evaluate(
                "sampleBatchIds=" + FormCodec.encode(incrementalSampleBatchId)
                        + "&targetColumns=Kingdom&baseModelSetVersion="
                        + FormCodec.encode(fullModelSetVersion));
        String modelSetVersion = requiredResult(incrementalTrainJson, "modelSetVersion");
        String detectRequest = "inputReference=" + FormCodec.encode("csv:" + dirtyPath)
                + "&modelSetVersion=" + FormCodec.encode(modelSetVersion)
                + "&rowKeyColumns=ID&errorsOnly=true";
        String detectJson = new F_DW_RAHADETECT().evaluate(detectRequest);
        String detectionBatchId = requiredResult(detectJson, "detectionBatchId");
        Map<String, Map<String, String>> dirtyRows = rowsById(spark, dirtyPath);
        Set<String> actual = actualErrors(dirtyRows, cleanRows);
        Set<String> predicted = predictedErrors(gateway, detectionBatchId);
        Map<String, Object> metrics = metrics(actual, predicted);
        Map<String, Object> report = new LinkedHashMap<String, Object>();
        report.put("datasetId", datasetId);
        report.put("sampleBatchId", sampleBatchId);
        report.put("incrementalSampleBatchId", incrementalSampleBatchId);
        report.put("fullModelSetVersion", fullModelSetVersion);
        report.put("modelSetVersion", modelSetVersion);
        report.put("detectionBatchId", detectionBatchId);
        report.put("sampleResult", sampleJson);
        report.put("trainResult", trainJson);
        report.put("incrementalSampleResult", incrementalSampleJson);
        report.put("incrementalTrainResult", incrementalTrainJson);
        report.put("detectResult", detectJson);
        report.put("actualErrors", actual);
        report.put("predictedErrors", predicted);
        report.put("metrics", metrics);
        report.put("tableCounts", tableCounts(gateway));
        report.put("elapsedMillis", System.currentTimeMillis() - startedAt);
        String reportJson = JsonUtils.toJson(report);
        Path path = Paths.get(reportPath);
        Files.createDirectories(path.getParent());
        LOGGER.info("写入 toy UDF 验证报告，path={}", reportPath);
        Files.write(path, reportJson.getBytes(StandardCharsets.UTF_8));
        double f1 = ((Number) metrics.get("f1")).doubleValue();
        if (f1 < 0.99d) {
            throw new IllegalStateException("toy 检测 F1 未达到验收值，report=" + reportJson);
        }
        System.out.println("RAHA_VALIDATION_REPORT=" + reportJson);
        spark.stop();
    }

    private static String requiredResult(String json, String field) {
        String errorCode = JsonUtils.getString(json, "errorCode");
        if (errorCode != null) {
            throw new IllegalStateException("UDF 执行失败，errorCode=" + errorCode
                    + "，message=" + JsonUtils.getString(json, "message"));
        }
        String value = JsonUtils.getString(json, field);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("UDF 返回缺少字段：" + field + "，json=" + json);
        }
        return value;
    }

    private static List<CellLabel> createLabels(SampleBatch batch,
                                                 List<SampleTuple> tuples,
                                                 Map<String, Map<String, String>> cleanRows) {
        long labeledAt = System.currentTimeMillis();
        String partitionDate = TimeUtils.partitionDate(batch.getCreatedAt(),
                RahaConfig.defaults().getPartitionTimeZone());
        List<CellLabel> labels = new ArrayList<CellLabel>();
        for (SampleTuple tuple : tuples) {
            Map<String, String> dirty = JsonUtils.parseStringMap(tuple.getRowDataJson());
            Map<String, String> clean = cleanRows.get(dirty.get("ID"));
            if (clean == null) {
                throw new IllegalStateException("干净数据缺少 ID=" + dirty.get("ID"));
            }
            for (String column : batch.getTargetColumns()) {
                String dirtyValue = ValueNormalizer.normalize(dirty.get(column));
                String cleanValue = ValueNormalizer.normalize(clean.get(column));
                int label = dirtyValue.equals(cleanValue) ? 0 : 1;
                labels.add(new CellLabel(batch.getSampleBatchId(), batch.getDatasetId(),
                        batch.getSnapshotId(), tuple.getRowId(), column,
                        HashUtils.sha256(dirtyValue), label, labeledAt, partitionDate));
            }
        }
        return labels;
    }

    private static Map<String, Map<String, String>> rowsById(SparkSession spark,
                                                              String path) {
        String csvPath = path.startsWith("/") && !path.startsWith("file:")
                ? "file:" + path : path;
        Dataset<Row> frame = spark.read().option("header", "true")
                .option("inferSchema", "false").csv(csvPath);
        Map<String, Map<String, String>> result = new LinkedHashMap<String, Map<String, String>>();
        for (Row row : frame.collectAsList()) {
            Map<String, String> values = new LinkedHashMap<String, String>();
            for (String column : frame.columns()) {
                Object value = row.getAs(column);
                values.put(column, value == null ? null : String.valueOf(value));
            }
            result.put(values.get("ID"), values);
        }
        return result;
    }

    private static Set<String> actualErrors(Map<String, Map<String, String>> dirty,
                                            Map<String, Map<String, String>> clean) {
        Set<String> errors = new HashSet<String>();
        for (Map.Entry<String, Map<String, String>> entry : dirty.entrySet()) {
            Map<String, String> cleanRow = clean.get(entry.getKey());
            for (String column : entry.getValue().keySet()) {
                String dirtyValue = ValueNormalizer.normalize(entry.getValue().get(column));
                String cleanValue = ValueNormalizer.normalize(cleanRow.get(column));
                if (!dirtyValue.equals(cleanValue)) {
                    errors.add(entry.getKey() + '|' + column);
                }
            }
        }
        return errors;
    }

    private static Set<String> predictedErrors(FmdbTableGateway gateway, String batchId) {
        Set<String> errors = new HashSet<String>();
        List<Row> rows = gateway.table(RahaTables.DETECTION_RESULT)
                .filter(col("detection_batch_id").equalTo(batchId)
                        .and(col("is_error").equalTo(true)))
                .select("row_id", "column_name").collectAsList();
        for (Row row : rows) {
            errors.add(row.getString(0) + '|' + row.getString(1));
        }
        return errors;
    }

    private static Map<String, Object> metrics(Set<String> actual, Set<String> predicted) {
        int truePositive = 0;
        for (String value : predicted) {
            if (actual.contains(value)) {
                truePositive++;
            }
        }
        int falsePositive = predicted.size() - truePositive;
        int falseNegative = actual.size() - truePositive;
        double precision = predicted.isEmpty() ? 0.0d
                : truePositive / (double) predicted.size();
        double recall = actual.isEmpty() ? 1.0d : truePositive / (double) actual.size();
        double f1 = precision + recall == 0.0d ? 0.0d
                : 2.0d * precision * recall / (precision + recall);
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("truePositive", truePositive);
        result.put("falsePositive", falsePositive);
        result.put("falseNegative", falseNegative);
        result.put("precision", precision);
        result.put("recall", recall);
        result.put("f1", f1);
        return result;
    }

    private static Map<String, Object> tableCounts(FmdbTableGateway gateway) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        values.put(RahaTables.SAMPLE_BATCH, gateway.table(RahaTables.SAMPLE_BATCH).count());
        values.put(RahaTables.SAMPLE_TUPLE, gateway.table(RahaTables.SAMPLE_TUPLE).count());
        values.put(RahaTables.CELL_LABEL, gateway.table(RahaTables.CELL_LABEL).count());
        values.put(RahaTables.MODEL_SET, gateway.table(RahaTables.MODEL_SET).count());
        values.put(RahaTables.COLUMN_MODEL, gateway.table(RahaTables.COLUMN_MODEL).count());
        values.put(RahaTables.TRAINING_EXAMPLE,
                gateway.table(RahaTables.TRAINING_EXAMPLE).count());
        values.put(RahaTables.DETECTION_BATCH,
                gateway.table(RahaTables.DETECTION_BATCH).count());
        values.put(RahaTables.DETECTION_RESULT,
                gateway.table(RahaTables.DETECTION_RESULT).count());
        return values;
    }
}
