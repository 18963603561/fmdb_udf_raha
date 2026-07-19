package com.fiberhome.ml.raha.annotation.service;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.annotation.excel.AnnotationExcelConfig;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow;
import com.fiberhome.ml.raha.fmdb.FmdbAnnotationRecordRepository;
import com.fiberhome.ml.raha.fmdb.FmdbPersistenceConfig;
import com.fiberhome.ml.raha.fmdb.FmdbPhysicalTable;
import com.fiberhome.ml.raha.fmdb.gateway.InMemoryFmdbTableGateway;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import java.nio.file.Path;
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
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 Excel 模板往返、标签展开、篡改拒绝和重复文件防重。 */
class AnnotationImportServiceIntegrationTest {

    /** 测试临时目录。 */
    @TempDir
    Path temporaryDirectory;

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldRoundTripLabelsAndRejectTamperedBusinessValue() throws Exception {
        AnnotationWorkbookAdapter adapter = new AnnotationWorkbookAdapter(
                AnnotationExcelConfig.defaults());
        FakeSampleRepository samples = new FakeSampleRepository(sampleRows());
        FakeAnnotationRepository annotations = new FakeAnnotationRepository();
        AnnotationTemplateService templateService = new AnnotationTemplateService(
                samples, adapter, fixedClock(1700000000000L));
        Path source = temporaryDirectory.resolve("annotation.xls");
        templateService.exportTemplate(new AnnotationTemplateRequest(
                "dataset-1", "2026-07", "sample-1", source));
        fillLabels(source);
        assertEquals("1", adapter.read(source).getRows().get(0).getRowLabel());

        AnnotationImportService service = new AnnotationImportService(samples,
                annotations, adapter, new AnnotationLabelExpander(),
                fixedClock(1700000000000L));
        AnnotationImportResult result = service.importWorkbook(
                new AnnotationImportRequest("dataset-1", "2026-07", "sample-1",
                        source, temporaryDirectory.resolve("errors.xls"),
                        "tester", false, null, null));

        assertEquals(AnnotationBatchStatus.IMPORTED, result.getStatus(),
                () -> describe(result));
        assertEquals(1, result.getCellLabels().size());
        assertEquals(1, annotations.saved.getRecords().size());
        assertEquals(1, result.getCellLabels().get(0).getLabel());
        assertEquals(1L, annotations.saveCount);
        assertFalse(result.getErrors().iterator().hasNext());

        Path tampered = temporaryDirectory.resolve("tampered.xls");
        adapter.exportTemplate(tampered, "dataset-1", "2026-07", "sample-1",
                sampleRows(), 1700000000000L);
        byte[] tamperedBytes;
        try (java.io.InputStream input = java.nio.file.Files.newInputStream(tampered);
             Workbook workbook = new HSSFWorkbook(input);
             java.io.ByteArrayOutputStream output =
                     new java.io.ByteArrayOutputStream()) {
            Sheet sheet = workbook.getSheet(AnnotationWorkbookAdapter.DATA_SHEET);
            sheet.getRow(1).getCell(5).setCellValue("changed");
            workbook.write(output);
            tamperedBytes = output.toByteArray();
        }
        java.nio.file.Files.write(tampered, tamperedBytes);
        AnnotationImportResult rejected = service.importWorkbook(
                new AnnotationImportRequest("dataset-1", "2026-07", "sample-1",
                        tampered, null, "tester", false, null, null));
        assertEquals(AnnotationBatchStatus.REJECTED, rejected.getStatus());
        assertTrue(rejected.getErrors().stream().anyMatch(error ->
                error.getErrorCode().name().equals("BUSINESS_DATA_CHANGED")));
        assertEquals(1L, annotations.saveCount);
    }

    @Test
    void shouldRejectSameFileTwiceWithoutAppending() {
        AnnotationWorkbookAdapter adapter = new AnnotationWorkbookAdapter(
                AnnotationExcelConfig.defaults());
        FakeSampleRepository samples = new FakeSampleRepository(sampleRows());
        FakeAnnotationRepository annotations = new FakeAnnotationRepository();
        AnnotationTemplateService templateService = new AnnotationTemplateService(
                samples, adapter, fixedClock(1700000000000L));
        Path source = temporaryDirectory.resolve("duplicate.xls");
        templateService.exportTemplate(new AnnotationTemplateRequest(
                "dataset-1", "2026-07", "sample-1", source));
        fillLabels(source);
        assertEquals("1", adapter.read(source).getRows().get(0).getRowLabel());
        AnnotationImportService service = new AnnotationImportService(samples,
                annotations, adapter, new AnnotationLabelExpander(),
                fixedClock(1700000000000L));
        AnnotationImportRequest request = new AnnotationImportRequest(
                "dataset-1", "2026-07", "sample-1", source, null,
                "tester", false, null, null);

        AnnotationImportResult first = service.importWorkbook(request);
        assertEquals(AnnotationBatchStatus.IMPORTED, first.getStatus(),
                () -> describe(first));
        assertEquals(AnnotationBatchStatus.DUPLICATE,
                service.importWorkbook(request).getStatus());
        assertEquals(1L, annotations.saveCount);
    }

    @Test
    void shouldReportMetadataAndAllRowValidationCodes() {
        AnnotationWorkbookAdapter adapter = new AnnotationWorkbookAdapter(
                AnnotationExcelConfig.defaults());
        List<SampleAnnotationRow> rows = validationSampleRows();
        FakeSampleRepository samples = new FakeSampleRepository(rows);
        Path metadataFile = temporaryDirectory.resolve("metadata-invalid.xls");
        adapter.exportTemplate(metadataFile, "dataset-1", "2026-07", "sample-1",
                rows, 1700000000000L);
        mutate(metadataFile, workbook -> {
            Sheet system = workbook.getSheet(AnnotationWorkbookAdapter.SYSTEM_SHEET);
            setSystemValue(system, "templateVersion", "old-version");
            setSystemValue(system, "datasetId", "other-dataset");
            setSystemValue(system, "schemaHash", "other-schema");
        });
        AnnotationImportService service = new AnnotationImportService(samples,
                new FakeAnnotationRepository(), adapter,
                new AnnotationLabelExpander(), fixedClock(1700000000000L));
        AnnotationImportResult metadataResult = service.importWorkbook(
                request(metadataFile, null));
        assertEquals(AnnotationBatchStatus.REJECTED, metadataResult.getStatus());
        assertCodes(metadataResult, "TEMPLATE_VERSION_INVALID",
                "SAMPLE_BATCH_MISMATCH", "SCHEMA_HASH_MISMATCH");

        Path rowFile = temporaryDirectory.resolve("row-invalid.xls");
        adapter.exportTemplate(rowFile, "dataset-1", "2026-07", "sample-1",
                rows, 1700000000000L);
        mutate(rowFile, workbook -> {
            Sheet sheet = workbook.getSheet(AnnotationWorkbookAdapter.DATA_SHEET);
            Row header = sheet.getRow(0);
            int rowId = findColumn(header, "_row_id");
            int taskId = findColumn(header, "_annotation_task_id");
            int hash = findColumn(header, "_row_content_hash");
            int label = findColumn(header, "_row_label");
            int errorColumns = findColumn(header, "_error_columns");
            int value = findColumn(header, "value");
            for (int index = 1; index <= 8; index++) {
                sheet.getRow(index).getCell(label).setCellValue("0");
            }
            sheet.getRow(1).getCell(rowId).setCellValue("");
            sheet.getRow(2).getCell(taskId).setCellValue("wrong-task");
            sheet.getRow(3).getCell(rowId).setCellValue("unknown-row");
            sheet.getRow(4).getCell(hash).setCellValue("wrong-hash");
            sheet.getRow(5).getCell(value).setCellValue("changed");
            sheet.getRow(6).getCell(label).setCellValue("2");
            sheet.getRow(7).getCell(label).setCellValue("1");
            sheet.getRow(7).getCell(errorColumns).setCellValue("id");
            sheet.getRow(8).getCell(rowId).setCellValue("row-2");
        });
        AnnotationImportResult rowResult = service.importWorkbook(
                request(rowFile, temporaryDirectory.resolve("row-errors.xls")));
        assertEquals(AnnotationBatchStatus.REJECTED, rowResult.getStatus());
        assertCodes(rowResult, "ROW_ID_MISSING", "ANNOTATION_TASK_MISMATCH",
                "ROW_NOT_IN_SAMPLE", "ROW_CONTENT_HASH_MISMATCH",
                "BUSINESS_DATA_CHANGED", "ROW_LABEL_INVALID",
                "ERROR_COLUMN_INVALID", "DUPLICATE_ROW");
        assertTrue(java.nio.file.Files.exists(rowResult.getValidationWorkbook()));
    }

    @Test
    void shouldPersistAndReloadAnnotationBatchThroughFmdbRepository() {
        AnnotationWorkbookAdapter adapter = new AnnotationWorkbookAdapter(
                AnnotationExcelConfig.defaults());
        FakeSampleRepository samples = new FakeSampleRepository(sampleRows());
        InMemoryFmdbTableGateway gateway = new InMemoryFmdbTableGateway(
                SparkTestSession.get());
        FmdbAnnotationRecordRepository repository =
                new FmdbAnnotationRecordRepository(SparkTestSession.get(), gateway,
                        FmdbPersistenceConfig.fromDefaults());
        Path source = temporaryDirectory.resolve("fmdb-roundtrip.xls");
        new AnnotationTemplateService(samples, adapter,
                fixedClock(1700000000000L)).exportTemplate(
                new AnnotationTemplateRequest("dataset-1", "2026-07",
                        "sample-1", source));
        fillLabels(source);
        AnnotationImportResult imported = new AnnotationImportService(samples,
                repository, adapter, new AnnotationLabelExpander(),
                fixedClock(1700000000000L)).importWorkbook(request(source, null));

        FmdbAnnotationRecordRepository restarted =
                new FmdbAnnotationRecordRepository(SparkTestSession.get(), gateway,
                        FmdbPersistenceConfig.fromDefaults());
        AnnotationBatch loaded = restarted.find("dataset-1",
                imported.getBatch().getPartitionMonth(),
                imported.getBatch().getAnnotationBatchId()).get();

        assertEquals(1, loaded.getRecords().size());
        assertEquals("snapshot-1", loaded.getRecords().get(0).getAnnotation()
                .getSourceSnapshotId());
        assertEquals(1, loaded.getRecords().get(0).getAnnotation()
                .getCellLabels().get(0).getLabel());
        assertEquals(0L, restarted.saveAll(loaded));
        assertEquals(1L, gateway.read(FmdbPhysicalTable.ANNOTATION_RECORD
                .getTableName()).count());
    }

    private static void fillLabels(Path path) {
        byte[] bytes;
        try (java.io.InputStream input = java.nio.file.Files.newInputStream(path);
             Workbook workbook = new HSSFWorkbook(input);
             java.io.ByteArrayOutputStream output =
                     new java.io.ByteArrayOutputStream()) {
            Sheet sheet = workbook.getSheet(AnnotationWorkbookAdapter.DATA_SHEET);
            Row row = sheet.getRow(1);
            Row header = sheet.getRow(0);
            row.getCell(findColumn(header, "_row_label")).setCellValue("1");
            row.getCell(findColumn(header, "_error_columns")).setCellValue("value");
            row.getCell(findColumn(header, "_comment")).setCellValue("测试异常");
            workbook.write(output);
            bytes = output.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
        try {
            java.nio.file.Files.write(path, bytes);
        } catch (java.io.IOException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static void mutate(Path path,
                               java.util.function.Consumer<Workbook> mutation) {
        byte[] bytes;
        try (java.io.InputStream input = java.nio.file.Files.newInputStream(path);
             Workbook workbook = new HSSFWorkbook(input);
             java.io.ByteArrayOutputStream output =
                     new java.io.ByteArrayOutputStream()) {
            mutation.accept(workbook);
            workbook.write(output);
            bytes = output.toByteArray();
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
        try {
            java.nio.file.Files.write(path, bytes);
        } catch (java.io.IOException exception) {
            throw new IllegalStateException(exception);
        }
    }

    private static void setSystemValue(Sheet sheet, String key, String value) {
        for (Row row : sheet) {
            if (key.equals(row.getCell(0).getStringCellValue())) {
                row.getCell(1).setCellValue(value);
                return;
            }
        }
        throw new IllegalStateException("测试系统信息缺少字段：" + key);
    }

    private static void assertCodes(AnnotationImportResult result,
                                    String... expectedCodes) {
        java.util.Set<String> actual = new java.util.LinkedHashSet<String>();
        result.getErrors().forEach(error -> actual.add(error.getErrorCode().name()));
        for (String code : expectedCodes) {
            assertTrue(actual.contains(code), () -> "缺少错误码 " + code
                    + "，实际为 " + actual);
        }
    }

    private static AnnotationImportRequest request(Path source,
                                                   Path validationOutput) {
        return new AnnotationImportRequest("dataset-1", "2026-07", "sample-1",
                source, validationOutput, "tester", false, null, null);
    }

    private static String describe(AnnotationImportResult result) {
        StringBuilder message = new StringBuilder();
        result.getErrors().forEach(error -> message.append(error.getErrorCode())
                .append(':').append(error.getMessage()).append(';'));
        return message.toString();
    }

    private static int findColumn(Row header, String name) {
        for (org.apache.poi.ss.usermodel.Cell cell : header) {
            if (name.equals(cell.getStringCellValue())) {
                return cell.getColumnIndex();
            }
        }
        throw new IllegalStateException("测试模板缺少字段：" + name);
    }

    private static List<SampleAnnotationRow> sampleRows() {
        Map<String, Object> schema = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
        columns.add(column("id", 0, false));
        columns.add(column("value", 1, true));
        schema.put("columns", columns);
        List<SampleAnnotationRow> rows = new ArrayList<SampleAnnotationRow>();
        rows.add(sample("row-1", "task-1", "A", "bad", schema));
        return rows;
    }

    private static List<SampleAnnotationRow> validationSampleRows() {
        Map<String, Object> schema = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
        columns.add(column("id", 0, false));
        columns.add(column("value", 1, true));
        schema.put("columns", columns);
        List<SampleAnnotationRow> rows = new ArrayList<SampleAnnotationRow>();
        for (int index = 1; index <= 8; index++) {
            rows.add(sample("row-" + index, "task-" + index,
                    "id-" + index, "value-" + index, schema));
        }
        return rows;
    }

    private static Map<String, Object> column(String name, int ordinal,
                                              boolean detectable) {
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("name", name);
        value.put("ordinal", ordinal);
        value.put("dataType", "string");
        value.put("nullable", true);
        value.put("detectable", detectable);
        value.put("sensitive", false);
        return value;
    }

    private static SampleAnnotationRow sample(String rowId, String taskId,
                                              String id, String value,
                                              Map<String, Object> schema) {
        Map<String, Object> rowData = new LinkedHashMap<String, Object>();
        rowData.put("id", id);
        rowData.put("value", value);
        Map<String, Object> context = new LinkedHashMap<String, Object>();
        context.put("snapshotId", "snapshot-1");
        return new SampleAnnotationRow(taskId, rowId, "hash-" + rowId,
                "schema-1", schema, rowData, context);
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }

    /** 测试使用的 c1 内存仓储。 */
    private static final class FakeSampleRepository implements SampleRecordRepository {

        /** 当前 c1 展示行。 */
        private final List<SampleAnnotationRow> rows;

        private FakeSampleRepository(List<SampleAnnotationRow> rows) {
            this.rows = rows;
        }

        @Override
        public boolean isPersistenceEnabled() { return true; }

        @Override
        public long saveAll(com.fiberhome.ml.raha.sampling.domain.SampleBatch batch) {
            return batch.getRecords().size();
        }

        @Override
        public Optional<com.fiberhome.ml.raha.sampling.domain.SampleBatch> find(
                String datasetId, String partitionMonth, String sampleBatchId) {
            return Optional.empty();
        }

        @Override
        public List<SampleAnnotationRow> findForAnnotation(String datasetId,
                                                            String partitionMonth,
                                                            String sampleBatchId) {
            return rows;
        }
    }

    /** 测试使用的标注内存仓储，保留最后一次追加批次。 */
    private static final class FakeAnnotationRepository
            implements AnnotationRecordRepository {

        /** 最后一次成功保存的批次。 */
        private AnnotationBatch saved;
        /** 成功追加次数。 */
        private long saveCount;

        @Override
        public boolean isPersistenceEnabled() { return true; }

        @Override
        public long saveAll(AnnotationBatch batch) {
            saved = batch;
            saveCount++;
            return batch.getRecords().size();
        }

        @Override
        public Optional<AnnotationBatch> find(String datasetId,
                                               String partitionMonth,
                                               String annotationBatchId) {
            return saved == null || !saved.getAnnotationBatchId().equals(
                    annotationBatchId) ? Optional.empty() : Optional.of(saved);
        }

        @Override
        public Optional<AnnotationBatch> findLatestForSample(String datasetId,
                                                              String partitionMonth,
                                                              String sampleBatchId) {
            return Optional.ofNullable(saved);
        }

        @Override
        public boolean existsImportFingerprint(String datasetId,
                                               String sampleBatchId,
                                               String importFingerprint) {
            return saved != null && importFingerprint.equals(
                    saved.getRecords().get(0).getImportFingerprint());
        }
    }
}
