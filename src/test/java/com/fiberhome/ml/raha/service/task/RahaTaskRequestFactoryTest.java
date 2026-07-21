package com.fiberhome.ml.raha.service.task;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRecord;
import com.fiberhome.ml.raha.annotation.domain.RowAnnotation;
import com.fiberhome.ml.raha.config.core.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.config.validation.ConfigVersioner;
import com.fiberhome.ml.raha.config.validation.RahaConfigFactory;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.identity.RowFingerprintAlgorithm;
import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.LabelSource;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.model.domain.ModelSetManifest;
import com.fiberhome.ml.raha.model.domain.RahaColumnModel;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.ModelSetRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import com.fiberhome.ml.raha.sampling.domain.SampleBatch;
import com.fiberhome.ml.raha.sampling.domain.SampleRecord;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证采样、持久化批次训练和显式模型集合检测的最小请求契约。
 */
class RahaTaskRequestFactoryTest {

    @Test
    void shouldCreateTableSamplingRequestWithConfiguredDefaults() {
        RahaTaskRequestFactory factory = factory(
                Collections.<SampleBatch>emptyList(),
                Collections.<AnnotationBatch>emptyList(),
                Collections.<String, ModelSetManifest>emptyMap());

        RahaTaskExecutionRequest request = factory.samplingTable(
                "`DW`.`ORDERS`");

        assertEquals("fmdb-table:dw.orders",
                request.getConfig().getDatasetId());
        assertEquals(DataFormat.FMDB_TABLE,
                request.getDataLoadRequest().getFormat());
        assertEquals(RowIdentityConfig.contentHash().getMode(),
                request.getDataLoadRequest().getRowIdentityConfig().getMode());
        assertEquals(1, request.getSamplingRound());
        assertEquals(RahaDefaultConfigProvider.factory().samplingConfig()
                        .getLabelingBudget(),
                request.getConfig().getSamplingConfig().getLabelingBudget());
        assertNotNull(request.getConfig().getExecutionInputFingerprint());
    }

    @Test
    void shouldApplySqlSamplingIdentityVersionAndBudgetOverrides() {
        RahaTaskRequestFactory factory = factory(
                Collections.<SampleBatch>emptyList(),
                Collections.<AnnotationBatch>emptyList(),
                Collections.<String, ModelSetManifest>emptyMap());
        FmdbInputSpec input = FmdbInputSpec.sql("orders-logical",
                        "SELECT * FROM dw.orders")
                .withRowKeyColumns("order_id")
                .withVersion("platform-snapshot-1", "source-v1");

        RahaTaskExecutionRequest request = factory.sampling(input,
                new SamplingRequestOptions(7, 2,
                        Collections.<CellLabel>emptyList()));

        assertEquals(DataFormat.FMDB_SQL,
                request.getDataLoadRequest().getFormat());
        assertEquals("dw.orders",
                request.getDataLoadRequest().getSourceReference());
        assertEquals(Collections.singletonList("order_id"),
                request.getDataLoadRequest().getRowIdentityConfig()
                        .getKeyColumns());
        assertEquals("platform-snapshot-1",
                request.getConfig().getSnapshotId());
        assertEquals("source-v1",
                request.getDataLoadRequest().getSourceVersion());
        assertEquals(7,
                request.getConfig().getSamplingConfig().getLabelingBudget());
        assertEquals(2, request.getSamplingRound());
    }

    @Test
    void shouldIncludeExistingLabelsInSamplingIdempotencyInput() {
        RahaTaskRequestFactory factory = factory(
                Collections.<SampleBatch>emptyList(),
                Collections.<AnnotationBatch>emptyList(),
                Collections.<String, ModelSetManifest>emptyMap());
        CellLabel existing = new CellLabel("cell-existing", 1,
                LabelSource.HUMAN, 1.0d, null, null, "tester", 1000L);

        RahaTaskExecutionRequest withoutLabels = factory.samplingTable(
                "dw.orders");
        RahaTaskExecutionRequest withLabels = factory.sampling(
                FmdbInputSpec.table("dw.orders"),
                new SamplingRequestOptions(null, 1,
                        Collections.singletonList(existing)));

        assertNotEquals(withoutLabels.getConfig().getExecutionInputFingerprint(),
                withLabels.getConfig().getExecutionInputFingerprint());
    }

    @Test
    void shouldResolveMultipleTrainingBatchesInStableOrder() {
        SampleBatch second = sampleBatch("sample-b", "annotation-row-b");
        SampleBatch first = sampleBatch("sample-a", "annotation-row-a");
        AnnotationBatch secondAnnotation = annotationBatch(second,
                "annotation-b");
        AnnotationBatch firstAnnotation = annotationBatch(first,
                "annotation-a");
        RahaTaskRequestFactory factory = factory(
                Arrays.asList(second, first),
                Arrays.asList(secondAnnotation, firstAnnotation),
                Collections.<String, ModelSetManifest>emptyMap());

        RahaTaskExecutionRequest reversed = factory.training(
                Arrays.asList("sample-b", "sample-a"),
                TrainingRequestOptions.defaults());
        RahaTaskExecutionRequest sorted = factory.training(
                Arrays.asList("sample-a", "sample-b"),
                TrainingRequestOptions.defaults());

        assertEquals(2, reversed.getTrainingBatchReferences().size());
        assertEquals("sample-a", reversed.getTrainingBatchReferences()
                .get(0).getSampleBatchId());
        assertEquals("sample-b", reversed.getTrainingBatchReferences()
                .get(1).getSampleBatchId());
        assertEquals(reversed.getConfig().getExecutionInputFingerprint(),
                sorted.getConfig().getExecutionInputFingerprint());
        assertEquals(DataFormat.FMDB_TABLE,
                reversed.getDataLoadRequest().getFormat());
        assertEquals(Collections.singletonList("id"),
                reversed.getRowIdentityConfig().getKeyColumns());
    }

    @Test
    void shouldRestoreTrainingSourceTypeFromSamplingContext() {
        SampleBatch sample = sampleBatch("sample-sql", "annotation-row",
                "dw.orders", DataFormat.FMDB_SQL);
        AnnotationBatch annotation = annotationBatch(sample, "annotation-sql");
        RahaTaskRequestFactory factory = factory(
                Collections.singletonList(sample),
                Collections.singletonList(annotation),
                Collections.<String, ModelSetManifest>emptyMap());

        RahaTaskExecutionRequest request = factory.training("sample-sql");

        assertEquals(DataFormat.FMDB_SQL,
                request.getDataLoadRequest().getFormat());
        assertEquals("SELECT * FROM dw.orders",
                request.getDataLoadRequest().getInputReference());
        assertEquals("dw.orders",
                request.getDataLoadRequest().getSourceReference());
    }

    @Test
    void shouldResolveLatestPublishedModelSetWhenDetectionVersionIsBlank() {
        RowIdentityConfig identity = RowIdentityConfig.sourceKey("order_id");
        ModelSetManifest older = manifest("dw.orders@20260721010101.000",
                identity, 1000L);
        ModelSetManifest latest = manifest("dw.orders@20260721020202.000",
                identity, 2000L);
        Map<String, ModelSetManifest> manifests =
                new LinkedHashMap<String, ModelSetManifest>();
        manifests.put(older.getModelSetVersion(), older);
        manifests.put(latest.getModelSetVersion(), latest);
        RahaTaskRequestFactory factory = factory(
                Collections.<SampleBatch>emptyList(),
                Collections.<AnnotationBatch>emptyList(), manifests);

        RahaTaskExecutionRequest request = factory.detectionTable("dw.orders");

        assertEquals(latest.getModelSetVersion(),
                request.getModelSetVersion());
        assertEquals("fmdb-table:dw.orders",
                request.getConfig().getDatasetId());
        assertEquals("dw.orders",
                request.getDataLoadRequest().getSourceReference());
    }

    @Test
    void shouldUseFirstSqlTableForDetectionDefaultModelLookup() {
        RowIdentityConfig identity = RowIdentityConfig.sourceKey("order_id");
        ModelSetManifest manifest = manifest("dw.orders@20260721010101.000",
                identity);
        RahaTaskRequestFactory factory = factory(
                Collections.<SampleBatch>emptyList(),
                Collections.<AnnotationBatch>emptyList(),
                Collections.singletonMap(manifest.getModelSetVersion(), manifest));

        RahaTaskExecutionRequest request = factory.detectionSql(
                "select * from dw.orders o join dw.customer c on o.id = c.id");

        assertEquals(manifest.getModelSetVersion(),
                request.getModelSetVersion());
        assertEquals("dw.orders",
                request.getDataLoadRequest().getSourceReference());
    }

    @Test
    void shouldRequireOverrideWhenLegacySamplingContextHasNoSourceType() {
        SampleBatch sample = sampleBatch("sample-legacy", "annotation-row",
                "dw.orders", null);
        AnnotationBatch annotation = annotationBatch(sample,
                "annotation-legacy");
        RahaTaskRequestFactory factory = factory(
                Collections.singletonList(sample),
                Collections.singletonList(annotation),
                Collections.<String, ModelSetManifest>emptyMap());

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> factory.training("sample-legacy"));

        assertTrue(exception.getMessage().contains("显式提供训练输入覆盖"));
    }

    @Test
    void shouldBindDetectionToPublishedModelSetAndIdempotencyInput() {
        RowIdentityConfig identity = RowIdentityConfig.sourceKey("order_id");
        ModelSetManifest first = manifest("model-set-1", identity);
        ModelSetManifest second = manifest("model-set-2", identity);
        Map<String, ModelSetManifest> manifests =
                new LinkedHashMap<String, ModelSetManifest>();
        manifests.put(first.getModelSetVersion(), first);
        manifests.put(second.getModelSetVersion(), second);
        RahaTaskRequestFactory factory = factory(
                Collections.<SampleBatch>emptyList(),
                Collections.<AnnotationBatch>emptyList(), manifests);

        RahaTaskExecutionRequest firstRequest = factory.detectionTable(
                "dw.orders_current", "model-set-1");
        RahaTaskExecutionRequest secondRequest = factory.detectionTable(
                "dw.orders_current", "model-set-2");

        assertEquals("orders-logical", firstRequest.getConfig().getDatasetId());
        assertEquals("model-set-1", firstRequest.getModelSetVersion());
        assertEquals(MissingModelPolicy.FAIL,
                firstRequest.getMissingModelPolicy());
        assertEquals(Collections.singletonList("order_id"),
                firstRequest.getDataLoadRequest().getRowIdentityConfig()
                        .getKeyColumns());
        assertNotEquals(new ConfigVersioner().versionOf(firstRequest.getConfig()),
                new ConfigVersioner().versionOf(secondRequest.getConfig()));
    }

    @Test
    void shouldIncludeMissingModelPolicyInDetectionIdempotencyInput() {
        RowIdentityConfig identity = RowIdentityConfig.sourceKey("order_id");
        ModelSetManifest manifest = manifest("model-set-1", identity);
        RahaTaskRequestFactory factory = factory(
                Collections.<SampleBatch>emptyList(),
                Collections.<AnnotationBatch>emptyList(),
                Collections.singletonMap("model-set-1", manifest));
        FmdbInputSpec input = new FmdbInputSpec("orders-logical",
                "dw.orders_current", "dw.orders_current",
                DataFormat.FMDB_TABLE, null, null, null,
                null, null, null, null);

        RahaTaskExecutionRequest fail = factory.detection(input,
                "model-set-1", new DetectionRequestOptions(
                        MissingModelPolicy.FAIL));
        RahaTaskExecutionRequest partial = factory.detection(input,
                "model-set-1", new DetectionRequestOptions(
                        MissingModelPolicy.PARTIAL));

        assertNotEquals(fail.getConfig().getExecutionInputFingerprint(),
                partial.getConfig().getExecutionInputFingerprint());
    }

    @Test
    void shouldRejectMutatingSqlAtMinimalInputBoundary() {
        assertThrows(IllegalArgumentException.class,
                () -> FmdbInputSpec.sql("orders-logical",
                        "DELETE FROM dw.orders"));
    }

    private static RahaTaskRequestFactory factory(
            List<SampleBatch> samples,
            List<AnnotationBatch> annotations,
            Map<String, ModelSetManifest> manifests) {
        return new RahaTaskRequestFactory(
                new RahaConfigFactory(RahaDefaultConfigProvider.properties()),
                new SampleRepository(samples),
                new AnnotationRepository(annotations),
                new ManifestRepository(manifests));
    }

    private static SampleBatch sampleBatch(String batchId, String rowId) {
        return sampleBatch(batchId, rowId, "dw.orders",
                DataFormat.FMDB_TABLE);
    }

    private static SampleBatch sampleBatch(String batchId,
                                           String rowId,
                                           String inputReference,
                                           DataFormat sourceType) {
        Map<String, Object> schema = new LinkedHashMap<String, Object>();
        schema.put("columns", Collections.singletonList(
                Collections.<String, Object>singletonMap("name", "value")));
        Map<String, Object> rowData = new LinkedHashMap<String, Object>();
        rowData.put("id", rowId);
        rowData.put("value", "value-" + rowId);
        Map<String, Object> context = new LinkedHashMap<String, Object>();
        context.put("snapshotId", "snapshot-v1");
        if (sourceType != null) {
            context.put(SampleRecord.SOURCE_TYPE_CONTEXT_KEY,
                    sourceType.name());
        }
        if (sourceType == DataFormat.FMDB_SQL) {
            context.put(SampleRecord.READ_INPUT_REFERENCE_CONTEXT_KEY,
                    "SELECT * FROM dw.orders");
        }
        SampleRecord record = new SampleRecord(batchId, "orders-logical",
                inputReference, "source-v1",
                RowIdentityConfig.sourceKey("id").getMode(),
                Collections.singletonList("id"),
                RowFingerprintAlgorithm.SHA_256,
                RowIdentityConfig.NORMALIZATION_VERSION, rowId,
                "content-" + rowId, "schema-v1", schema, rowData, 1L,
                "sampling-v1", context, 1000L, "2026-07");
        return new SampleBatch(batchId, "orders-logical", "snapshot-v1",
                "source-v1", "sampling-v1", 1000L, "2026-07",
                Collections.singletonList(record));
    }

    private static AnnotationBatch annotationBatch(SampleBatch sample,
                                                    String annotationBatchId) {
        SampleRecord sampleRecord = sample.getRecords().get(0);
        CellLabel label = new CellLabel("cell-" + sampleRecord.getRowId(),
                1, LabelSource.HUMAN, 1.0d, null, null,
                "tester", 1500L);
        RowAnnotation annotation = new RowAnnotation("task-1",
                sampleRecord.getRowId(), sample.getSnapshotId(),
                sampleRecord.getRowContentHash(), 1,
                Collections.singleton("value"),
                Collections.singleton("value"), "错误",
                Collections.singletonList(label));
        AnnotationRecord record = new AnnotationRecord(annotationBatchId,
                sample.getSampleBatchId(), sample.getDatasetId(), annotation,
                sampleRecord.getRowData(), "template-v1", "labels.xlsx",
                sampleRecord.getSchemaHash(), "tester",
                AnnotationBatchStatus.IMPORTED, 1L, 1L, 0L, null,
                1500L, "2026-07", "fingerprint-" + annotationBatchId);
        return new AnnotationBatch(annotationBatchId,
                sample.getSampleBatchId(), sample.getDatasetId(),
                AnnotationBatchStatus.IMPORTED, 1500L, "2026-07", null,
                Collections.singletonList(record));
    }

    private static ModelSetManifest manifest(String modelSetVersion,
                                             RowIdentityConfig identity) {
        return manifest(modelSetVersion, identity, 1200L);
    }

    private static ModelSetManifest manifest(String modelSetVersion,
                                             RowIdentityConfig identity,
                                             long publishedAt) {
        RahaColumnModel model = new RahaColumnModel("orders-value",
                "model-" + modelSetVersion, datasetId(modelSetVersion), "value",
                "schema-v1", ClassifierType.LOGISTIC_REGRESSION,
                "dictionary-v1", "plan-v1", 0.5d,
                "memory://" + modelSetVersion, ModelStatus.PUBLISHED,
                Collections.<String, Double>emptyMap(), 1000L, publishedAt,
                modelSetVersion, identity);
        return new ModelSetManifest(modelSetVersion,
                Collections.singletonList(model));
    }

    private static String datasetId(String modelSetVersion) {
        return modelSetVersion.startsWith("dw.orders@")
                ? "fmdb-table:dw.orders" : "orders-logical";
    }

    /** 内存测试采样仓储。 */
    private static final class SampleRepository
            implements SampleRecordRepository {
        /** 按批次标识保存测试采样。 */
        private final Map<String, SampleBatch> values =
                new LinkedHashMap<String, SampleBatch>();

        private SampleRepository(List<SampleBatch> samples) {
            for (SampleBatch sample : samples) {
                values.put(sample.getSampleBatchId(), sample);
            }
        }

        @Override public boolean isPersistenceEnabled() { return true; }
        @Override public long saveAll(SampleBatch batch) { return 0L; }
        @Override public Optional<SampleBatch> find(String datasetId,
                                                     String partitionMonth,
                                                     String sampleBatchId) {
            return Optional.ofNullable(values.get(sampleBatchId));
        }
        @Override public Optional<SampleBatch> findByBatchId(String sampleBatchId) {
            return Optional.ofNullable(values.get(sampleBatchId));
        }
        @Override public List<SampleAnnotationRow> findForAnnotation(
                String datasetId, String partitionMonth, String sampleBatchId) {
            return Collections.emptyList();
        }
    }

    /** 内存测试标注仓储。 */
    private static final class AnnotationRepository
            implements AnnotationRecordRepository {
        /** 按采样批次标识保存测试标注。 */
        private final Map<String, AnnotationBatch> values =
                new LinkedHashMap<String, AnnotationBatch>();

        private AnnotationRepository(List<AnnotationBatch> annotations) {
            for (AnnotationBatch annotation : annotations) {
                values.put(annotation.getSampleBatchId(), annotation);
            }
        }

        @Override public boolean isPersistenceEnabled() { return true; }
        @Override public long saveAll(AnnotationBatch batch) { return 0L; }
        @Override public Optional<AnnotationBatch> find(String datasetId,
                                                         String partitionMonth,
                                                         String annotationBatchId) {
            for (AnnotationBatch value : values.values()) {
                if (annotationBatchId.equals(value.getAnnotationBatchId())) {
                    return Optional.of(value);
                }
            }
            return Optional.empty();
        }
        @Override public Optional<AnnotationBatch> findLatestForSample(
                String datasetId, String partitionMonth, String sampleBatchId) {
            return Optional.ofNullable(values.get(sampleBatchId));
        }
        @Override public Optional<AnnotationBatch> findLatestTrainableForSample(
                String sampleBatchId, boolean allowPartial) {
            return Optional.ofNullable(values.get(sampleBatchId));
        }
        @Override public boolean existsImportFingerprint(String datasetId,
                                                          String sampleBatchId,
                                                          String fingerprint) {
            return false;
        }
    }

    /** 内存测试模型集合仓储。 */
    private static final class ManifestRepository implements ModelSetRepository {
        /** 按版本保存模型集合清单。 */
        private final Map<String, ModelSetManifest> values;

        private ManifestRepository(Map<String, ModelSetManifest> values) {
            this.values = values;
        }

        @Override public Optional<ModelSetManifest> find(String version) {
            return Optional.ofNullable(values.get(version));
        }

        @Override public Optional<ModelSetManifest> findLatestPublishedByDataset(
                String datasetId) {
            ModelSetManifest selected = null;
            for (ModelSetManifest manifest : values.values()) {
                if (!datasetId.equals(manifest.getDatasetId())
                        || !manifest.isPublished()) {
                    continue;
                }
                if (selected == null || latestPublishedAt(manifest)
                        > latestPublishedAt(selected)) {
                    selected = manifest;
                }
            }
            return Optional.ofNullable(selected);
        }

        private static long latestPublishedAt(ModelSetManifest manifest) {
            long value = 0L;
            for (RahaColumnModel model : manifest.getModelsByColumn().values()) {
                value = Math.max(value, model.getPublishedAt().longValue());
            }
            return value;
        }
    }
}
