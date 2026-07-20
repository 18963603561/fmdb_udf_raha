package com.fiberhome.ml.raha.service.train;

import com.fiberhome.ml.raha.data.loader.identity.RowFingerprintAlgorithm;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRecord;
import com.fiberhome.ml.raha.annotation.domain.RowAnnotation;
import com.fiberhome.ml.raha.annotation.service.AnnotationLabelExpander;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.DatasetSnapshot;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityColumns;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityResult;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityService;
import com.fiberhome.ml.raha.data.loader.metadata.SchemaHasher;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 c1 优先合并、内容冲突审计和训练快照标签坐标转换。 */
class TrainingInputMergeServiceIntegrationTest {

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldMergeMultipleBatchesAndRemapLabelsToTrainingSnapshot() {
        SparkSession spark = SparkTestSession.get();
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true);
        RowIdentityConfig identity = RowIdentityConfig.sourceKey("id");
        Dataset<Row> c1Raw = spark.createDataFrame(Arrays.asList(
                RowFactory.create("A", "old"),
                RowFactory.create("B", "b-value"),
                RowFactory.create("D", "c1-only")), schema);
        RowIdentityResult c1Identified = new RowIdentityService().identify(
                c1Raw, identity);
        Dataset<Row> o1Raw = spark.createDataFrame(Arrays.asList(
                RowFactory.create("A", "new"),
                RowFactory.create("B", "b-value"),
                RowFactory.create("C", "c-value")), schema);
        RowIdentityResult o1Identified = new RowIdentityService().identify(
                o1Raw, identity);
        String schemaHash = new SchemaHasher().hash(schema);
        List<ColumnMetadata> columns = Arrays.asList(
                new ColumnMetadata("id", 0, "string", false, false, false),
                new ColumnMetadata("value", 1, "string", true, true, false));
        RahaDataset original = new RahaDataset("dataset-1", "o1-snapshot",
                "orders", RowIdentityColumns.ROW_ID, columns,
                o1Identified.getDataFrame(), schemaHash, Collections.emptyMap());
        SampleRecord first = sampleRecord(c1Identified.getDataFrame().filter(
                "id = 'A'").collectAsList().get(0), schemaHash, "sample-1",
                "A", "old", "c1-snapshot", columns);
        SampleRecord second = sampleRecord(c1Identified.getDataFrame().filter(
                "id = 'B'").collectAsList().get(0), schemaHash, "sample-1",
                "B", "b-value", "c1-snapshot", columns);
        SampleRecord third = sampleRecord(c1Identified.getDataFrame().filter(
                "id = 'D'").collectAsList().get(0), schemaHash, "sample-2",
                "D", "c1-only", "c1-snapshot", columns);
        SampleBatch firstSampleBatch = new SampleBatch("sample-1", "dataset-1",
                "c1-snapshot", "source-v1", "sampling-v1", 1000L,
                "2026-07", Arrays.asList(first, second));
        SampleBatch secondSampleBatch = new SampleBatch("sample-2", "dataset-1",
                "c1-snapshot", "source-v1", "sampling-v1", 1000L,
                "2026-07", Collections.singletonList(third));
        AnnotationBatch firstAnnotation = annotationBatch(first, schemaHash,
                "sample-1", "annotation-1");
        AnnotationBatch secondAnnotation = annotationBatch(third, schemaHash,
                "sample-2", "annotation-2");
        TrainingInputMergeService service = new TrainingInputMergeService(spark,
                new SampleRepository(Arrays.asList(firstSampleBatch,
                        secondSampleBatch)),
                new AnnotationRepository(Arrays.asList(firstAnnotation,
                        secondAnnotation)), fixedClock(2000L));

        TrainingMergeResult result = service.merge(new TrainingMergeRequest(
                "training-1", original, identity, Arrays.asList(
                new TrainingBatchReference("sample-1", "2026-07",
                        "annotation-1", "2026-07"),
                new TrainingBatchReference("sample-2", "2026-07",
                        "annotation-2", "2026-07")), 1024L * 1024L));

        assertEquals(4L, result.getMetrics().getMergedCount());
        assertEquals(2L, result.getMetrics().getMatchedIdentityCount());
        assertEquals(1L, result.getMetrics().getKeyContentConflictCount());
        assertEquals(2L, result.getMetrics().getDedupRowCount());
        assertEquals(1L, result.getMetrics().getC1OnlyCount());
        assertEquals(4L, result.getDataset().getDataFrame().count());
        assertEquals(2, result.getDirectLabels().size());
        String expectedCell = new CellCoordinate("dataset-1",
                result.getTrainingSnapshotId(), first.getRowId(), "value").toCellId();
        assertEquals(expectedCell, result.getDirectLabels().get(0).getCellId());
        assertNotEquals(new CellCoordinate("dataset-1", "c1-snapshot",
                first.getRowId(), "value").toCellId(),
                result.getDirectLabels().get(0).getCellId());
        assertTrue(result.getTrainingContext().containsKey("dedupRowCount"));
        assertEquals(Arrays.asList("sample-1", "sample-2"),
                result.getTrainingContext().get("sampleBatchIds"));
        assertEquals("old", result.getDataset().getDataFrame()
                .filter("id = 'A'").select("value").first().getString(0));
    }

    private static SampleRecord sampleRecord(Row identified,
                                             String schemaHash,
                                             String sampleBatchId,
                                             String id,
                                             String value,
                                             String snapshotId,
                                             List<ColumnMetadata> columns) {
        Map<String, Object> rowData = new LinkedHashMap<String, Object>();
        rowData.put("id", id);
        rowData.put("value", value);
        Map<String, Object> schema = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> definitions = new ArrayList<Map<String, Object>>();
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
        schema.put("columns", definitions);
        Map<String, Object> context = new LinkedHashMap<String, Object>();
        context.put("snapshotId", snapshotId);
        return new SampleRecord(sampleBatchId, "dataset-1", "orders",
                "source-v1", RowIdentityConfig.sourceKey("id").getMode(),
                Collections.singletonList("id"),
                com.fiberhome.ml.raha.data.loader.identity.RowFingerprintAlgorithm.SHA_256,
                RowIdentityConfig.NORMALIZATION_VERSION,
                identified.getAs(RowIdentityColumns.ROW_ID),
                identified.getAs(RowIdentityColumns.ROW_CONTENT_HASH), schemaHash,
                schema, rowData, 1L, "sampling-v1", context, 1000L, "2026-07");
    }

    private static AnnotationBatch annotationBatch(SampleRecord record,
                                                    String schemaHash,
                                                    String sampleBatchId,
                                                    String annotationBatchId) {
        AnnotationLabelExpander expander = new AnnotationLabelExpander();
        List<com.fiberhome.ml.raha.label.CellLabel> labels = expander.expand(
                "dataset-1", "c1-snapshot", record.getRowId(), 1,
                Collections.singletonList("value"),
                Collections.singleton("value"), "tester", 1500L,
                annotationBatchId);
        RowAnnotation annotation = new RowAnnotation("task-1", record.getRowId(),
                "c1-snapshot", record.getRowContentHash(), 1,
                Collections.singleton("value"), Collections.singleton("value"),
                "人工异常", labels);
        AnnotationRecord row = new AnnotationRecord(annotationBatchId,
                sampleBatchId,
                "dataset-1", annotation, record.getRowData(), "xls-v1",
                "label.xls", schemaHash, "tester", AnnotationBatchStatus.IMPORTED,
                1L, 1L, 0L, null, 1500L, "2026-07", "fingerprint-1");
        return new AnnotationBatch(annotationBatchId, sampleBatchId, "dataset-1",
                AnnotationBatchStatus.IMPORTED, 1500L, "2026-07", null,
                Collections.singletonList(row));
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    private static final class SampleRepository implements SampleRecordRepository {

        /** 按批次标识保存测试采样批次。 */
        private final Map<String, SampleBatch> batches =
                new LinkedHashMap<String, SampleBatch>();

        private SampleRepository(List<SampleBatch> values) {
            for (SampleBatch value : values) {
                batches.put(value.getSampleBatchId(), value);
            }
        }
        @Override public boolean isPersistenceEnabled() { return true; }
        @Override public long saveAll(SampleBatch value) { return value.getRecords().size(); }
        @Override public Optional<SampleBatch> find(String datasetId,
                                                    String partitionMonth,
                                                    String sampleBatchId) {
            return Optional.ofNullable(batches.get(sampleBatchId));
        }
        @Override public List<com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow>
                findForAnnotation(String datasetId, String partitionMonth,
                                  String sampleBatchId) {
            return Collections.emptyList();
        }
    }

    private static final class AnnotationRepository
            implements AnnotationRecordRepository {

        /** 按标注批次标识保存测试标注批次。 */
        private final Map<String, AnnotationBatch> batches =
                new LinkedHashMap<String, AnnotationBatch>();

        private AnnotationRepository(List<AnnotationBatch> values) {
            for (AnnotationBatch value : values) {
                batches.put(value.getAnnotationBatchId(), value);
            }
        }
        @Override public boolean isPersistenceEnabled() { return true; }
        @Override public long saveAll(AnnotationBatch value) { return value.getRecords().size(); }
        @Override public Optional<AnnotationBatch> find(String datasetId,
                                                         String partitionMonth,
                                                         String annotationBatchId) {
            return Optional.ofNullable(batches.get(annotationBatchId));
        }
        @Override public Optional<AnnotationBatch> findLatestForSample(
                String datasetId, String partitionMonth, String sampleBatchId) {
            for (AnnotationBatch batch : batches.values()) {
                if (sampleBatchId.equals(batch.getSampleBatchId())) {
                    return Optional.of(batch);
                }
            }
            return Optional.empty();
        }
        @Override public boolean existsImportFingerprint(String datasetId,
                                                         String sampleBatchId,
                                                         String importFingerprint) {
            return false;
        }
    }
}
