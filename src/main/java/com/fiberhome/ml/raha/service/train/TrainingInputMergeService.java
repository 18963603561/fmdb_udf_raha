package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRecord;
import com.fiberhome.ml.raha.annotation.domain.RowAnnotation;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityMode;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.data.profile.ColumnProfiler;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ReadableIdUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 读取 c1 和 o1，按统一行身份执行 c1 优先去重，并重建训练标签坐标。
 */
public final class TrainingInputMergeService {

    /** 合并规则版本，进入训练批次上下文和快照版本。 */
    public static final String MERGE_ALGORITHM_VERSION = "merge-v1";
    /** 临时来源字段，不会泄露给下游训练。 */
    private static final String MERGE_SOURCE = "_raha_merge_source";
    /** 临时排序字段。 */
    private static final String MERGE_ORDER = "_raha_merge_order";
    /** 临时去重序号字段。 */
    private static final String MERGE_ROW_NUMBER = "_raha_merge_row_number";
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            TrainingInputMergeService.class);
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** c1 采样仓储。 */
    private final SampleRecordRepository sampleRepository;
    /** 标注批次仓储。 */
    private final AnnotationRecordRepository annotationRepository;
    /** 提供可测试合并时间的时钟。 */
    private final Clock clock;
    /** 合并后训练快照的画像计算器。 */
    private final ColumnProfiler columnProfiler;

    public TrainingInputMergeService(SparkSession sparkSession,
                                     SampleRecordRepository sampleRepository,
                                     AnnotationRecordRepository annotationRepository,
                                     Clock clock) {
        this(sparkSession, sampleRepository, annotationRepository, clock,
                new ColumnProfiler());
    }

    public TrainingInputMergeService(SparkSession sparkSession,
                                     SampleRecordRepository sampleRepository,
                                     AnnotationRecordRepository annotationRepository,
                                     Clock clock,
                                     ColumnProfiler columnProfiler) {
        if (sparkSession == null || sampleRepository == null
                || annotationRepository == null || clock == null
                || columnProfiler == null) {
            throw new IllegalArgumentException("训练合并服务依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.sampleRepository = sampleRepository;
        this.annotationRepository = annotationRepository;
        this.clock = clock;
        this.columnProfiler = columnProfiler;
    }

    /**
     * 加载指定批次并生成训练快照。
     */
    public TrainingMergeResult merge(TrainingMergeRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("训练合并请求不能为空");
        }
        long startedAt = clock.millis();
        if (startedAt <= 0L) {
            throw new IllegalStateException("训练合并时钟必须返回正时间");
        }
        RahaDataset original = request.getOriginalDataset();
        LOGGER.info("开始合并训练输入，trainingBatchId={}，datasetId={}，"
                        + "batchCount={}，snapshotId={}",
                request.getTrainingBatchId(), original.getDatasetId(),
                request.getBatchReferences().size(),
                original.getSnapshotId());
        List<SampleBatch> samples = new ArrayList<SampleBatch>();
        List<AnnotationBatch> annotations = new ArrayList<AnnotationBatch>();
        for (TrainingBatchReference reference : request.getBatchReferences()) {
            SampleBatch sample = loadSample(request, reference);
            AnnotationBatch annotation = loadAnnotation(request, reference);
            validateIdentityAndSchema(original, sample,
                    request.getRowIdentityConfig());
            samples.add(sample);
            annotations.add(annotation);
        }
        List<SampleRecord> sampleRecords = uniqueSampleRecords(
                samples, request.getRowIdentityConfig());
        Dataset<Row> originalFrame = original.getDataFrame();
        long originalCount = originalFrame.count();
        Dataset<Row> sampleFrame = sampleFrame(originalFrame.schema(), sampleRecords);
        Dataset<Row> taggedOriginal = originalFrame.withColumn(MERGE_SOURCE,
                functions.lit("O1"));
        Dataset<Row> taggedSample = sampleFrame.withColumn(MERGE_SOURCE,
                functions.lit("C1"));
        String identityColumn = request.getRowIdentityConfig().getMode()
                == RowIdentityMode.CONTENT_HASH
                ? RowIdentityColumns.ROW_CONTENT_HASH
                : RowIdentityColumns.ROW_ID;
        Dataset<Row> rawUnion = taggedSample.unionByName(taggedOriginal);
        Dataset<Row> originalDeduplicated = deduplicateOriginal(
                taggedOriginal, identityColumn, originalFrame.schema());
        Dataset<Row> c1Keys = taggedSample.select(identityColumn).distinct();
        long estimatedBytes = estimateSampleBytes(sampleRecords);
        boolean broadcastC1 = estimatedBytes <= request.getBroadcastThresholdBytes();
        Dataset<Row> joinKeys = broadcastC1 ? functions.broadcast(c1Keys) : c1Keys;
        Column joinCondition = originalDeduplicated.alias("o1")
                .col(identityColumn).eqNullSafe(joinKeys.alias("c1")
                        .col(identityColumn));
        Dataset<Row> originalWithoutC1 = originalDeduplicated.alias("o1")
                .join(joinKeys.alias("c1"), joinCondition, "left_anti");
        Dataset<Row> union = taggedSample.unionByName(originalWithoutC1);
        Dataset<Row> merged = union
                .withColumn(MERGE_ORDER, functions.when(
                        functions.col(MERGE_SOURCE).equalTo("C1"), 0).otherwise(1))
                .drop(MERGE_SOURCE, MERGE_ORDER)
                .selectExpr(originalFrame.schema().fieldNames());
        long mergedCount = merged.count();
        TrainingMergeMetrics metrics = metrics(rawUnion, identityColumn,
                sampleRecords.size(), originalCount, mergedCount);
        LOGGER.info("训练 c1 广播决策，trainingBatchId={}，sampleCount={}，"
                        + "estimatedBytes={}，thresholdBytes={}，broadcast={}",
                request.getTrainingBatchId(), sampleRecords.size(), estimatedBytes,
                request.getBroadcastThresholdBytes(), broadcastC1);
        if (metrics.getKeyContentConflictCount() > 0L) {
            LOGGER.warn("训练合并存在联合键内容冲突，trainingBatchId={}，"
                            + "conflictGroupCount={}，sampleCount={}，originalCount={}",
                    request.getTrainingBatchId(),
                    metrics.getKeyContentConflictCount(), metrics.getSampleCount(),
                    metrics.getOriginalCount());
        }
        LOGGER.info("训练输入合并完成，trainingBatchId={}，identityMode={}，"
                        + "sampleCount={}，originalCount={}，mergedCount={}，"
                        + "dedupGroupCount={}，dedupRowCount={}，c1OnlyCount={}",
                request.getTrainingBatchId(), request.getRowIdentityConfig().getMode(),
                metrics.getSampleCount(), metrics.getOriginalCount(),
                metrics.getMergedCount(), metrics.getDedupGroupCount(),
                metrics.getDedupRowCount(), metrics.getC1OnlyCount());
        String trainingSnapshotId = ReadableIdUtils.prefixedVersion("train",
                original.getTableName(), startedAt);
        RahaDataset trainingDataset = new RahaDataset(original.getDatasetId(),
                trainingSnapshotId, original.getTableName(), original.getRowIdColumn(),
                original.getColumns(), merged, original.getSchemaHash(),
                Collections.emptyMap());
        // 合并后的集合必须重新画像，不能把 o1 或采样快照画像直接当作训练画像。
        trainingDataset = trainingDataset.withProfiles(
                request.isColumnBatchChild()
                        ? columnProfiler.profileDetectable(trainingDataset)
                        : columnProfiler.profile(trainingDataset));
        List<CellLabel> labels = remapLabels(annotations, original.getDatasetId(),
                trainingSnapshotId);
        Map<String, Object> context = context(request, original, samples, annotations,
                metrics, trainingSnapshotId, startedAt);
        return new TrainingMergeResult(trainingDataset, labels,
                request.getTrainingBatchId(), trainingSnapshotId,
                MERGE_ALGORITHM_VERSION, metrics, context);
    }

    private static Dataset<Row> deduplicateOriginal(Dataset<Row> taggedOriginal,
                                                     String identityColumn,
                                                     StructType originalSchema) {
        WindowSpec groups = Window.partitionBy(functions.col(identityColumn));
        return taggedOriginal
                .withColumn(MERGE_ORDER, functions.lit(1))
                .withColumn(MERGE_ROW_NUMBER, functions.row_number().over(
                        groups.orderBy(functions.col(RowIdentityColumns.ROW_CONTENT_HASH)
                                .asc())))
                .filter(functions.col(MERGE_ROW_NUMBER).equalTo(1))
                .drop(MERGE_ORDER, MERGE_ROW_NUMBER);
    }

    private static long estimateSampleBytes(List<SampleRecord> records) {
        long bytes = 0L;
        for (SampleRecord record : records) {
            bytes = Math.addExact(bytes, FmdbJsonCodec.write(record.getRowData())
                    .getBytes(StandardCharsets.UTF_8).length);
            bytes = Math.addExact(bytes, record.getRowId().length()
                    + record.getRowContentHash().length());
        }
        return bytes;
    }

    private SampleBatch loadSample(TrainingMergeRequest request,
                                   TrainingBatchReference reference) {
        Optional<SampleBatch> result = sampleRepository.find(
                request.getOriginalDataset().getDatasetId(),
                reference.getSamplePartitionMonth(),
                reference.getSampleBatchId());
        if (!result.isPresent()) {
            throw new IllegalStateException("训练指定的 c1 采样批次不存在");
        }
        return result.get();
    }

    private AnnotationBatch loadAnnotation(TrainingMergeRequest request,
                                           TrainingBatchReference reference) {
        Optional<AnnotationBatch> result = annotationRepository.find(
                request.getOriginalDataset().getDatasetId(),
                reference.getAnnotationPartitionMonth(),
                reference.getAnnotationBatchId());
        if (!result.isPresent()) {
            throw new IllegalStateException("训练指定的标注批次不存在");
        }
        AnnotationBatch batch = result.get();
        if (!reference.getSampleBatchId().equals(batch.getSampleBatchId())
                || !request.getOriginalDataset().getDatasetId().equals(
                batch.getDatasetId())) {
            throw new IllegalArgumentException("标注批次与训练采样或数据集不一致");
        }
        return batch;
    }

    private static void validateIdentityAndSchema(RahaDataset original,
                                                  SampleBatch sample,
                                                  RowIdentityConfig config) {
        if (!original.getDatasetId().equals(sample.getDatasetId())
                || !original.getSchemaHash().equals(sample.getRecords().get(0)
                .getSchemaHash())) {
            throw new IllegalArgumentException("c1 与 o1 数据集或模式哈希不一致");
        }
        SampleRecord first = sample.getRecords().get(0);
        for (SampleRecord record : sample.getRecords()) {
            if (record.getRowIdentityMode() != config.getMode()
                    || !record.getRowKeyColumns().equals(config.getKeyColumns())
                    || record.getFingerprintAlgorithm()
                    != config.getFingerprintAlgorithm()
                    || !record.getFingerprintVersion().equals(
                    config.getNormalizationVersion())
                    || !original.getSchemaHash().equals(record.getSchemaHash())) {
                throw new IllegalArgumentException("c1 与 o1 行身份配置或模式不一致");
            }
        }
        List<ColumnMetadata> expected = original.getColumns();
        Map<String, Map<String, Object>> definitions = schemaDefinitions(
                first.getColumnSchema());
        if (definitions.size() != expected.size()) {
            throw new IllegalArgumentException("c1 与 o1 字段数量不一致");
        }
        for (ColumnMetadata column : expected) {
            Map<String, Object> definition = definitions.get(column.getName());
            if (definition == null
                    || !column.getDataType().equals(String.valueOf(
                    definition.get("dataType")))
                    || column.getOrdinal() != ((Number) definition.get(
                    "ordinal")).intValue()
                    || column.isNullable() != Boolean.TRUE.equals(
                    definition.get("nullable"))
                    // 列批子任务可以收窄可检测字段，但不能启用采样时已禁用的字段。
                    || (column.isDetectable()
                    && !Boolean.TRUE.equals(definition.get("detectable")))) {
                throw new IllegalArgumentException("c1 与 o1 字段模式或检测属性不兼容："
                        + column.getName());
            }
        }
    }

    private Dataset<Row> sampleFrame(StructType originalSchema,
                                     List<SampleRecord> records) {
        List<Row> rows = new ArrayList<Row>(records.size());
        for (SampleRecord record : records) {
            Object[] values = new Object[originalSchema.fields().length];
            for (int index = 0; index < values.length; index++) {
                StructField field = originalSchema.fields()[index];
                if (RowIdentityColumns.ROW_ID.equals(field.name())) {
                    values[index] = record.getRowId();
                } else if (RowIdentityColumns.ROW_CONTENT_HASH.equals(field.name())) {
                    values[index] = record.getRowContentHash();
                } else if (RowIdentityColumns.DUPLICATE_COUNT.equals(field.name())) {
                    values[index] = record.getDuplicateCount();
                } else if (!record.getRowData().containsKey(field.name())) {
                    throw new IllegalArgumentException("c1 原始行缺少字段："
                            + field.name());
                } else {
                    values[index] = convert(record.getRowData().get(field.name()),
                            field.dataType());
                }
            }
            rows.add(RowFactory.create(values));
        }
        return sparkSession.createDataFrame(rows, originalSchema);
    }

    private static TrainingMergeMetrics metrics(Dataset<Row> union,
                                                String identityColumn,
                                                long sampleCount,
                                                long originalCount,
                                                long mergedCount) {
        Dataset<Row> groups = union.groupBy(functions.col(identityColumn))
                .agg(functions.count(functions.lit(1L)).alias("row_count"),
                        functions.countDistinct(functions.col(MERGE_SOURCE))
                                .alias("source_count"),
                        functions.countDistinct(functions.col(
                                RowIdentityColumns.ROW_CONTENT_HASH))
                                .alias("content_count"),
                        functions.max(functions.when(functions.col(MERGE_SOURCE)
                                .equalTo("C1"), 1).otherwise(0)).alias("has_c1"));
        long dedupGroups = groups.filter(functions.col("row_count").gt(1L)).count();
        long matched = groups.filter(functions.col("source_count").gt(1L)).count();
        long conflicts = RowIdentityColumns.ROW_ID.equals(identityColumn)
                ? groups.filter(functions.col("content_count").gt(1L)).count() : 0L;
        long c1Only = groups.filter(functions.col("has_c1").equalTo(1)
                .and(functions.col("source_count").equalTo(1))).count();
        return new TrainingMergeMetrics(sampleCount, originalCount, mergedCount,
                matched, dedupGroups, union.count() - mergedCount, conflicts, c1Only);
    }

    private static List<CellLabel> remapLabels(List<AnnotationBatch> batches,
                                               String datasetId,
                                               String trainingSnapshotId) {
        List<CellLabel> labels = new ArrayList<CellLabel>();
        Map<String, CellLabel> sourceByCell =
                new LinkedHashMap<String, CellLabel>();
        for (AnnotationBatch batch : batches) {
            for (AnnotationRecord record : batch.getRecords()) {
                RowAnnotation annotation = record.getAnnotation();
                List<String> columns = new ArrayList<String>(
                        annotation.getReviewedColumns());
                for (int index = 0; index < columns.size(); index++) {
                    CellLabel source = annotation.getCellLabels().get(index);
                    String column = columns.get(index);
                    String sourceCell = annotation.getRowId() + "\u0000" + column;
                    CellLabel existing = sourceByCell.get(sourceCell);
                    if (existing != null && existing.getLabel() != source.getLabel()) {
                        throw new IllegalArgumentException("多批次训练包含冲突标签："
                                + annotation.getRowId() + "/" + column);
                    }
                    if (existing != null) {
                        continue;
                    }
                    sourceByCell.put(sourceCell, source);
                    String cellId = new CellCoordinate(datasetId,
                            trainingSnapshotId, annotation.getRowId(), column)
                            .toCellId();
                    String labelId = HashUtils.md5Hex(source.getLabelId() + "|"
                            + trainingSnapshotId);
                    labels.add(new CellLabel(labelId, cellId, source.getLabel(),
                            source.getLabelSource(), source.getConfidence(), null,
                            null, null, null, 1.0d, 0, null,
                            source.getAnnotator(), source.getCreatedAt()));
                }
            }
        }
        return Collections.unmodifiableList(labels);
    }

    private static Map<String, Object> context(TrainingMergeRequest request,
                                               RahaDataset original,
                                               List<SampleBatch> samples,
                                               List<AnnotationBatch> annotations,
                                               TrainingMergeMetrics metrics,
                                               String trainingSnapshotId,
                                               long sourceReadAt) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        List<String> sampleBatchIds = new ArrayList<String>();
        List<String> annotationBatchIds = new ArrayList<String>();
        for (SampleBatch sample : samples) {
            sampleBatchIds.add(sample.getSampleBatchId());
        }
        for (AnnotationBatch annotation : annotations) {
            annotationBatchIds.add(annotation.getAnnotationBatchId());
        }
        result.put("trainingBatchId", request.getTrainingBatchId());
        result.put("trainingSnapshotId", trainingSnapshotId);
        result.put("sampleBatchId", joinIds(sampleBatchIds));
        result.put("annotationBatchId", joinIds(annotationBatchIds));
        result.put("sampleBatchIds", sampleBatchIds);
        result.put("annotationBatchIds", annotationBatchIds);
        result.put("inputReference", original.getTableName());
        result.put("sourceReadAt", sourceReadAt);
        result.put("sampleDataCount", metrics.getSampleCount());
        result.put("originalDataCount", metrics.getOriginalCount());
        result.put("matchedIdentityCount", metrics.getMatchedIdentityCount());
        result.put("dedupGroupCount", metrics.getDedupGroupCount());
        result.put("dedupRowCount", metrics.getDedupRowCount());
        result.put("mergedCount", metrics.getMergedCount());
        result.put("mergeAlgorithmVersion", MERGE_ALGORITHM_VERSION);
        result.put("rowIdentityMode", request.getRowIdentityConfig().getMode().name());
        return result;
    }

    private static List<SampleRecord> uniqueSampleRecords(
            List<SampleBatch> samples,
            RowIdentityConfig identityConfig) {
        Map<String, SampleRecord> unique =
                new LinkedHashMap<String, SampleRecord>();
        for (SampleBatch sample : samples) {
            for (SampleRecord record : sample.getRecords()) {
                String identity = identityConfig.getMode()
                        == RowIdentityMode.CONTENT_HASH
                        ? record.getRowContentHash() : record.getRowId();
                SampleRecord existing = unique.get(identity);
                if (existing != null && !existing.getRowContentHash().equals(
                        record.getRowContentHash())) {
                    throw new IllegalArgumentException("多批次训练包含相同行身份的内容冲突："
                            + identity);
                }
                if (existing == null) {
                    unique.put(identity, record);
                }
            }
        }
        List<SampleRecord> result = new ArrayList<SampleRecord>(unique.values());
        Collections.sort(result, new Comparator<SampleRecord>() {
            @Override
            public int compare(SampleRecord first, SampleRecord second) {
                return first.getRowId().compareTo(second.getRowId());
            }
        });
        return Collections.unmodifiableList(result);
    }

    private static String joinIds(List<String> values) {
        StringBuilder result = new StringBuilder();
        for (String value : values) {
            if (result.length() > 0) {
                result.append(',');
            }
            result.append(value);
        }
        return result.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Map<String, Object>> schemaDefinitions(
            Map<String, Object> schema) {
        Object raw = schema.get("columns");
        if (!(raw instanceof List)) {
            throw new IllegalArgumentException("c1 字段模式缺少 columns");
        }
        List<Map<String, Object>> definitions =
                new ArrayList<Map<String, Object>>((List<Map<String, Object>>) raw);
        Collections.sort(definitions, new Comparator<Map<String, Object>>() {
            @Override
            public int compare(Map<String, Object> first, Map<String, Object> second) {
                return Integer.compare(((Number) first.get("ordinal")).intValue(),
                        ((Number) second.get("ordinal")).intValue());
            }
        });
        Map<String, Map<String, Object>> result =
                new LinkedHashMap<String, Map<String, Object>>();
        for (Map<String, Object> definition : definitions) {
            result.put(String.valueOf(definition.get("name")), definition);
        }
        return result;
    }

    private static Object convert(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        if (type.equals(DataTypes.StringType)) {
            return String.valueOf(value);
        }
        if (type.equals(DataTypes.BooleanType)) {
            return value instanceof Boolean ? value
                    : Boolean.valueOf(String.valueOf(value));
        }
        if (type.equals(DataTypes.ByteType)) return ((Number) value).byteValue();
        if (type.equals(DataTypes.ShortType)) return ((Number) value).shortValue();
        if (type.equals(DataTypes.IntegerType)) return ((Number) value).intValue();
        if (type.equals(DataTypes.LongType)) return ((Number) value).longValue();
        if (type.equals(DataTypes.FloatType)) return ((Number) value).floatValue();
        if (type.equals(DataTypes.DoubleType)) return ((Number) value).doubleValue();
        if (type instanceof DecimalType) return new BigDecimal(String.valueOf(value));
        if (type.equals(DataTypes.DateType)) return Date.valueOf(String.valueOf(value));
        if (type.equals(DataTypes.TimestampType)) return Timestamp.from(
                Instant.parse(String.valueOf(value)));
        if (type instanceof ArrayType || type instanceof MapType) return value;
        return value;
    }
}
