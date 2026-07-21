package com.fiberhome.ml.raha.sampling.service;

import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPartitionUtils;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.AnnotationTask;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import com.fiberhome.ml.raha.util.ReadableIdUtils;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

/**
 * 从可信 Raha 数据集回取采样完整行，生成 c1 批次并验证物理持久化。
 */
public final class SampleRecordService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SampleRecordService.class);
    /** c1 物理仓储。 */
    private final SampleRecordRepository repository;
    /** 提供可测试批次时间的时钟。 */
    private final Clock clock;

    public SampleRecordService(SampleRecordRepository repository, Clock clock) {
        if (repository == null || clock == null) {
            throw new IllegalArgumentException("采样记录服务依赖不能为空");
        }
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 回取采样完整业务行并幂等写入最终采样表。
     *
     * <p>该重载用于兼容旧手工装配，不保存来源类型。需要通过最小训练入口恢复输入时，
     * 应使用带 {@link DataFormat} 参数的重载。</p>
     */
    public SampleMaterializationResult materializeAndPersist(
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            RowIdentityConfig identityConfig,
            SamplingBatchResult sampling) {
        return materializeAndPersist(dataset, snapshot, identityConfig, null,
                null, sampling);
    }

    /**
     * 回取采样完整业务行，并把调用入口明确选择的来源类型写入持久化上下文。
     *
     * @param dataset 已完成行身份处理的可信数据集
     * @param snapshot 当前输入快照
     * @param identityConfig 行身份规则
     * @param sourceType 表、SQL 或文件等明确来源类型；为空时按旧协议不保存
     * @param sampling 当前采样结果
     * @return 采样批次物化和持久化结果
     */
    public SampleMaterializationResult materializeAndPersist(
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            RowIdentityConfig identityConfig,
            DataFormat sourceType,
            SamplingBatchResult sampling) {
        return materializeAndPersist(dataset, snapshot, identityConfig,
                sourceType, null, sampling);
    }

    /**
     * 回取采样完整业务行，并把展示来源和实际读取引用分别写入持久化上下文。
     *
     * @param dataset 已完成行身份处理的可信数据集
     * @param snapshot 当前输入快照
     * @param identityConfig 行身份规则
     * @param sourceType 表、SQL 或文件等明确来源类型；为空时按旧协议不保存
     * @param readInputReference 实际读取引用，SQL 来源时保存 SQL 原文
     * @param sampling 当前采样结果
     * @return 采样批次物化和持久化结果
     */
    public SampleMaterializationResult materializeAndPersist(
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            RowIdentityConfig identityConfig,
            DataFormat sourceType,
            String readInputReference,
            SamplingBatchResult sampling) {
        if (dataset == null || snapshot == null || identityConfig == null
                || sampling == null || sampling.getTasks().isEmpty()) {
            throw new IllegalArgumentException("c1 物化输入和采样任务必须有效");
        }
        if (!dataset.getDatasetId().equals(snapshot.getDatasetId())
                || !dataset.getSnapshotId().equals(snapshot.getSnapshotId())) {
            throw new IllegalArgumentException("c1 数据集和快照不一致");
        }
        List<AnnotationTask> tasks = sampling.getTasks();
        Set<String> rowIds = new LinkedHashSet<String>();
        Map<String, AnnotationTask> taskByRow =
                new LinkedHashMap<String, AnnotationTask>();
        for (AnnotationTask task : tasks) {
            if (task == null || !sampling.getSamplingVersion().equals(
                    task.getSamplingVersion()) || !rowIds.add(task.getRowId())) {
                throw new IllegalArgumentException("采样任务版本或行标识重复");
            }
            taskByRow.put(task.getRowId(), task);
        }
        long createdAt = Math.max(1L, clock.millis());
        String partitionMonth = FmdbPartitionUtils.month(createdAt);
        String sampleBatchId = batchId(dataset, createdAt);
        LOGGER.info("开始物化 c1 采样批次，sampleBatchId={}，datasetId={}，"
                        + "selectedRowCount={}，samplingVersion={}", sampleBatchId,
                dataset.getDatasetId(), rowIds.size(), sampling.getSamplingVersion());
        SampleBatch batch = buildBatch(sampleBatchId, dataset, snapshot,
                identityConfig, sourceType, readInputReference, sampling,
                taskByRow, createdAt, partitionMonth);
        if (!repository.isPersistenceEnabled()) {
            LOGGER.info("c1 采样物理表开关已关闭，仅完成当前任务内存物化，"
                            + "sampleBatchId={}，recordCount={}", sampleBatchId,
                    batch.getRecords().size());
            return new SampleMaterializationResult(batch, 0L, false, null);
        }
        long written = repository.saveAll(batch);
        SampleBatch stored = repository.find(dataset.getDatasetId(),
                        partitionMonth, sampleBatchId)
                .orElseThrow(() -> new IllegalStateException(
                        "c1 采样批次写入后无法回读"));
        if (stored.getRecords().size() != batch.getRecords().size()) {
            throw new IllegalStateException("c1 采样批次物理记录数量不完整");
        }
        String location = "fmdb://dw.raha_sample_record/dataset_id/"
                + dataset.getDatasetId() + "/partition_month/" + partitionMonth
                + "/sample_batch_id/" + sampleBatchId;
        LOGGER.info("c1 采样批次物化完成，sampleBatchId={}，inputCount={}，"
                        + "writtenCount={}，storedCount={}，resultLocation={}",
                sampleBatchId, batch.getRecords().size(), written,
                stored.getRecords().size(), location);
        return new SampleMaterializationResult(batch, written, true, location);
    }

    private static SampleBatch buildBatch(
            String sampleBatchId,
            RahaDataset dataset,
            DatasetSnapshot snapshot,
            RowIdentityConfig identityConfig,
            DataFormat sourceType,
            String readInputReference,
            SamplingBatchResult sampling,
            Map<String, AnnotationTask> taskByRow,
            long createdAt,
            String partitionMonth) {
        Dataset<Row> selected = dataset.getDataFrame().filter(
                functions.col(dataset.getRowIdColumn()).isin(
                        taskByRow.keySet().toArray(new Object[0])));
        List<Row> rows = selected.collectAsList();
        if (rows.size() != taskByRow.size()) {
            throw new IllegalStateException("采样任务无法一一回取可信逻辑行");
        }
        Map<String, Object> columnSchema = columnSchema(dataset.getColumns());
        StructType frameSchema = dataset.getDataFrame().schema();
        List<SampleRecord> records = new ArrayList<SampleRecord>(rows.size());
        for (Row row : rows) {
            String rowId = String.valueOf((Object) row.getAs(
                    dataset.getRowIdColumn()));
            AnnotationTask task = taskByRow.get(rowId);
            if (task == null) {
                throw new IllegalStateException("回取的采样行不属于当前采样任务");
            }
            String contentHash = requiredString(row,
                    RowIdentityColumns.ROW_CONTENT_HASH);
            long duplicateCount = ((Number) row.getAs(
                    RowIdentityColumns.DUPLICATE_COUNT)).longValue();
            records.add(new SampleRecord(sampleBatchId, dataset.getDatasetId(),
                    snapshot.getInputReference(), snapshot.getSourceVersion(),
                    identityConfig.getMode(), identityConfig.getKeyColumns(),
                    identityConfig.getFingerprintAlgorithm(),
                    identityConfig.getNormalizationVersion(), rowId, contentHash,
                    dataset.getSchemaHash(), columnSchema,
                    rowData(row, frameSchema, dataset.getColumns()), duplicateCount,
                    sampling.getSamplingVersion(), samplingContext(task,
                    snapshot.getSnapshotId(), sourceType, readInputReference),
                    createdAt, partitionMonth));
        }
        Collections.sort(records, new Comparator<SampleRecord>() {
            @Override
            public int compare(SampleRecord first, SampleRecord second) {
                return first.getRowId().compareTo(second.getRowId());
            }
        });
        return new SampleBatch(sampleBatchId, dataset.getDatasetId(),
                dataset.getSnapshotId(), snapshot.getSourceVersion(),
                sampling.getSamplingVersion(), createdAt, partitionMonth, records);
    }

    private static String batchId(RahaDataset dataset, long createdAt) {
        return ReadableIdUtils.prefixedVersion("sample",
                dataset.getTableName(), createdAt);
    }

    private static Map<String, Object> columnSchema(List<ColumnMetadata> columns) {
        List<Map<String, Object>> definitions =
                new ArrayList<Map<String, Object>>(columns.size());
        for (ColumnMetadata column : columns) {
            Map<String, Object> definition = new LinkedHashMap<String, Object>();
            definition.put("name", column.getName());
            definition.put("ordinal", column.getOrdinal());
            definition.put("dataType", column.getDataType());
            definition.put("nullable", column.isNullable());
            definition.put("detectable", column.isDetectable());
            definition.put("sensitive", column.isSensitive());
            definitions.add(definition);
        }
        Map<String, Object> schema = new LinkedHashMap<String, Object>();
        schema.put("columns", definitions);
        return schema;
    }

    private static Map<String, Object> rowData(Row row,
                                               StructType schema,
                                               List<ColumnMetadata> columns) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        for (ColumnMetadata column : columns) {
            StructField field = schema.apply(column.getName());
            values.put(column.getName(), jsonValue(
                    row.getAs(column.getName()), field.dataType()));
        }
        return values;
    }

    private static Map<String, Object> samplingContext(AnnotationTask task,
                                                       String snapshotId,
                                                       DataFormat sourceType,
                                                       String readInputReference) {
        Map<String, Object> context = new LinkedHashMap<String, Object>();
        context.put("annotationTaskId", task.getTaskId());
        context.put("jobId", task.getJobId());
        context.put("samplingRound", task.getSamplingRound());
        context.put("samplingScore", task.getSamplingScore());
        context.put("coveredClusters", task.getCoveredClusters());
        context.put("taskStatus", task.getStatus().name());
        context.put("expiresAt", task.getExpiresAt());
        context.put("snapshotId", snapshotId);
        if (sourceType != null) {
            context.put(SampleRecord.SOURCE_TYPE_CONTEXT_KEY,
                    sourceType.name());
        }
        if (sourceType == DataFormat.FMDB_SQL && readInputReference != null
                && !readInputReference.trim().isEmpty()) {
            context.put(SampleRecord.READ_INPUT_REFERENCE_CONTEXT_KEY,
                    readInputReference);
        }
        return context;
    }

    private static Object jsonValue(Object value, DataType type) {
        if (value == null || type.equals(DataTypes.StringType)
                || type.equals(DataTypes.BooleanType)
                || type.equals(DataTypes.ByteType)
                || type.equals(DataTypes.ShortType)
                || type.equals(DataTypes.IntegerType)
                || type.equals(DataTypes.LongType)
                || type.equals(DataTypes.FloatType)
                || type.equals(DataTypes.DoubleType)) {
            return value;
        }
        if (type instanceof org.apache.spark.sql.types.DecimalType) {
            BigDecimal decimal = value instanceof Decimal
                    ? ((Decimal) value).toJavaBigDecimal() : (BigDecimal) value;
            return decimal.toPlainString();
        }
        if (type.equals(DataTypes.DateType)) {
            return ((Date) value).toLocalDate().toString();
        }
        if (type.equals(DataTypes.TimestampType)) {
            return ((Timestamp) value).toInstant().toString();
        }
        if (type.equals(DataTypes.BinaryType)) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        if (type instanceof ArrayType) {
            return jsonArray(value, (ArrayType) type);
        }
        if (type instanceof MapType) {
            return jsonMap(value, (MapType) type);
        }
        if (type instanceof StructType && value instanceof Row) {
            Map<String, Object> nested = new LinkedHashMap<String, Object>();
            StructType nestedType = (StructType) type;
            Row nestedRow = (Row) value;
            for (int index = 0; index < nestedType.fields().length; index++) {
                nested.put(nestedType.fields()[index].name(), jsonValue(
                        nestedRow.get(index), nestedType.fields()[index].dataType()));
            }
            return nested;
        }
        return String.valueOf(value);
    }

    private static List<Object> jsonArray(Object value, ArrayType type) {
        List<Object> result = new ArrayList<Object>();
        if (value instanceof Seq) {
            for (Object element : JavaConverters.seqAsJavaList((Seq<?>) value)) {
                result.add(jsonValue(element, type.elementType()));
            }
            return result;
        }
        for (Object element : (List<?>) value) {
            result.add(jsonValue(element, type.elementType()));
        }
        return result;
    }

    private static Map<String, Object> jsonMap(Object value, MapType type) {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        if (value instanceof scala.collection.Map) {
            Iterator<?> iterator = ((scala.collection.Map<?, ?>) value).iterator();
            while (iterator.hasNext()) {
                Tuple2<?, ?> entry = (Tuple2<?, ?>) iterator.next();
                result.put(String.valueOf(entry._1()),
                        jsonValue(entry._2(), type.valueType()));
            }
            return result;
        }
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            result.put(String.valueOf(entry.getKey()),
                    jsonValue(entry.getValue(), type.valueType()));
        }
        return result;
    }

    private static String requiredString(Row row, String columnName) {
        Object value = row.getAs(columnName);
        if (value == null || String.valueOf(value).trim().isEmpty()) {
            throw new IllegalStateException("采样可信输入缺少技术字段：" + columnName);
        }
        return String.valueOf(value);
    }
}
