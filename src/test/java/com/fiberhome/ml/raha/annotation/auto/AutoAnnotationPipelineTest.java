package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.annotation.excel.AnnotationExcelConfig;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookData;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import com.sun.net.httpserver.HttpServer;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证自动标注配置、双层分批、响应校验、合并、工作簿回写和编排闭环。
 */
class AutoAnnotationPipelineTest {

    /** 测试临时目录。 */
    @TempDir
    Path temporaryDirectory;

    @Test
    void shouldParseConfigAndRejectInvalidEnabledConnection() {
        Map<String, String> values = configValues();
        AutoAnnotationConfig config = AutoAnnotationConfig.from(values);

        assertTrue(config.isEnabled());
        assertEquals(2, config.getMaxRowsPerBatch());
        assertEquals(1, config.getMaxColumnsPerBatch());
        assertEquals(AutoAnnotationFailPolicy.PARTIAL,
                config.getFailPolicy());
        config.validateEnabled();

        values.remove("autoLabelModelUrl");
        AutoAnnotationConfig invalid = AutoAnnotationConfig.from(values);
        assertThrows(IllegalArgumentException.class, invalid::validateEnabled);
    }

    @Test
    void shouldDeriveBatchDefaultsFromQwenContextCapacity() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("autoLabelEnabled", "false");
        values.put("autoLabelModel", "qwen3.5code");

        AutoAnnotationConfig config = AutoAnnotationConfig.from(values);

        assertEquals("QWEN_CODER_256K", config.getModelCapacityProfile());
        assertEquals(262144, config.getContextWindowTokens());
        assertEquals(32768, config.getMaxOutputTokens());
        assertEquals(162201, config.getMaxCharsPerBatch());
        assertEquals(179, config.getMaxRowsPerBatch());
        assertEquals(200, config.getMaxColumnsPerBatch());

        values.put("autoLabelContextWindowTokens", "65536");
        AutoAnnotationConfig overridden = AutoAnnotationConfig.from(values);
        assertEquals(65536, overridden.getContextWindowTokens());
        assertEquals(16384, overridden.getMaxOutputTokens());
        assertEquals(89, overridden.getMaxRowsPerBatch());
    }

    @Test
    void shouldSplitByRowsAndColumnsWithinSerializedCharacterLimit() {
        AnnotationWorkbookData workbook = workbookData();
        AutoAnnotationConfig config = AutoAnnotationConfig.from(configValues());
        List<AutoAnnotationBatch> batches = new AutoAnnotationBatchBuilder(
                new LlmPromptBuilder()).build(workbook, "dataset-1", "sample-1",
                Collections.singleton("email"), config);

        assertEquals(4, batches.size());
        for (AutoAnnotationBatch batch : batches) {
            assertTrue(batch.getRows().size() <= 2);
            assertEquals(1, batch.getDetectableColumns().size());
            assertTrue(batch.getEstimatedChars()
                    <= config.getMaxCharsPerBatch());
        }
        String maskedPrompt = new LlmPromptBuilder().buildUserPrompt(
                "dataset-1", "sample-1", batches.get(2), true,
                Collections.singleton("email"), config.getMaxValueChars());
        assertTrue(maskedPrompt.contains("<MASKED:length="));
        assertFalse(maskedPrompt.contains("a@example.com"));
    }

    @Test
    void shouldValidateStrictResponseAndRejectMissingRows() {
        AutoAnnotationBatch batch = batch("batch-1",
                Arrays.asList(workbookData().getRows().get(0),
                        workbookData().getRows().get(1)),
                Collections.singletonList("value"));
        String valid = "{\"batchId\":\"batch-1\",\"items\":["
                + "{\"rowId\":\"row-1\",\"rowLabel\":1,"
                + "\"errorColumns\":[\"value\"],\"confidence\":0.9,"
                + "\"reason\":\"格式异常\"},"
                + "{\"rowId\":\"row-2\",\"rowLabel\":0,"
                + "\"errorColumns\":[],\"confidence\":0.8,"
                + "\"reason\":\"正常\"}]}";
        assertEquals(2, new LlmResponseValidator().validate(valid, batch).size());

        String missing = "{\"batchId\":\"batch-1\",\"items\":["
                + "{\"rowId\":\"row-1\",\"rowLabel\":0,"
                + "\"errorColumns\":[],\"confidence\":0.9,"
                + "\"reason\":\"正常\"}]}";
        assertThrows(IllegalArgumentException.class,
                () -> new LlmResponseValidator().validate(missing, batch));
    }

    @Test
    void shouldMergeColumnWindowsAndKeepFailedNormalRowBlank() {
        AnnotationWorkbookData workbook = workbookData();
        AutoAnnotationBatch valueBatch = batch("batch-1",
                workbook.getRows(), Collections.singletonList("value"));
        AutoAnnotationBatch emailBatch = batch("batch-2",
                workbook.getRows(), Collections.singletonList("email"));
        List<AutoAnnotationDecision> valueDecisions = Arrays.asList(
                decision("batch-1", "row-1", 1, "value"),
                decision("batch-1", "row-2", 0, null),
                decision("batch-1", "row-3", 0, null));
        AutoAnnotationBatchResult success = new AutoAnnotationBatchResult(
                valueBatch, true, 1, 10L, valueDecisions, null);
        AutoAnnotationBatchResult failed = new AutoAnnotationBatchResult(
                emailBatch, false, 3, 20L,
                Collections.<AutoAnnotationDecision>emptyList(), "超时");

        List<AutoAnnotationDecision> merged =
                new AutoAnnotationMergeService().merge(workbook,
                        Arrays.asList(success, failed));

        assertEquals(1, merged.size());
        assertEquals("row-1", merged.get(0).getRowId());
        assertEquals(Collections.singletonList("value"),
                merged.get(0).getErrorColumns());
        assertTrue(merged.get(0).isRequiresReview());
    }

    @Test
    void shouldRunEndToEndAndProduceImportCompatibleWorkbookAndReports()
            throws Exception {
        AnnotationWorkbookAdapter adapter = new AnnotationWorkbookAdapter(
                AnnotationExcelConfig.defaults());
        Path source = temporaryDirectory.resolve("raha-annotation_sample-1_t.xls");
        Path target = temporaryDirectory.resolve(
                "raha-annotation_sample-1_t_auto.xls");
        adapter.exportTemplate(source, "dataset-1", "2026-07", "sample-1",
                "snapshot-1", sampleRows(), 1700000000000L);
        LlmAutoAnnotationService service = new LlmAutoAnnotationService(adapter,
                Clock.fixed(Instant.ofEpochMilli(1700000000000L),
                        ZoneOffset.UTC), new DeterministicClient());

        AutoAnnotationResult result = service.autoLabel(
                new AutoAnnotationRequest(source, target,
                        temporaryDirectory.resolve("auto-label"), "dataset-1",
                        "sample-1", Collections.singleton("email")),
                AutoAnnotationConfig.from(configValues()));

        assertEquals(AutoAnnotationStatus.SUCCEEDED, result.getStatus());
        assertEquals(3, result.getLabeledCount());
        assertTrue(Files.exists(target));
        assertTrue(Files.exists(result.getSummaryPath()));
        assertTrue(Files.exists(result.getDecisionsPath()));
        assertTrue(Files.exists(result.getBatchesPath()));
        AnnotationWorkbookData output = adapter.read(target);
        assertEquals("1", output.getRows().get(0).getRowLabel());
        assertEquals("value", output.getRows().get(0).getErrorColumns());
        assertEquals("bad", output.getRows().get(0)
                .getBusinessValues().get("value"));
        try (java.io.InputStream input = Files.newInputStream(target);
             Workbook workbook = new HSSFWorkbook(input)) {
            assertNotNull(workbook.getSheet(
                    AnnotationAutoLabelWorkbookWriter.DETAIL_SHEET));
        }
    }

    @Test
    void shouldRetryAndKeepRawTemplateUnderWarnOnly() {
        AnnotationWorkbookAdapter adapter = new AnnotationWorkbookAdapter(
                AnnotationExcelConfig.defaults());
        Path source = temporaryDirectory.resolve("warn-source.xls");
        Path target = temporaryDirectory.resolve("warn-auto.xls");
        adapter.exportTemplate(source, "dataset-1", "2026-07", "sample-1",
                "snapshot-1", sampleRows(), 1700000000000L);
        Map<String, String> values = configValues();
        values.put("autoLabelMaxRetryCount", "1");
        values.put("autoLabelFailPolicy", "WARN_ONLY");
        values.put("autoLabelMaxRowsPerBatch", "20");
        values.put("autoLabelMaxColumnsPerBatch", "40");
        CountingFailClient client = new CountingFailClient();

        AutoAnnotationResult result = new LlmAutoAnnotationService(adapter,
                Clock.systemUTC(), client).autoLabel(new AutoAnnotationRequest(
                        source, target, temporaryDirectory.resolve("warn-report"),
                        "dataset-1", "sample-1", Collections.<String>emptySet()),
                AutoAnnotationConfig.from(values));

        assertEquals(AutoAnnotationStatus.FAILED, result.getStatus());
        assertFalse(Files.exists(target));
        assertEquals(2, client.callCount);
        assertTrue(Files.exists(result.getSummaryPath()));
    }

    @Test
    void shouldCallOpenCompatibleEndpointWithBearerKey() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(
                "127.0.0.1", 0), 0);
        AtomicReference<String> authorization = new AtomicReference<String>();
        AtomicReference<String> requestBody = new AtomicReference<String>();
        server.createContext("/v1/chat/completions", exchange -> {
            authorization.set(exchange.getRequestHeaders().getFirst(
                    "Authorization"));
            byte[] request = readAll(exchange.getRequestBody());
            requestBody.set(new String(request,
                    java.nio.charset.StandardCharsets.UTF_8));
            String response = "{\"choices\":[{\"message\":{\"content\":"
                    + "\"{\\\"batchId\\\":\\\"batch-1\\\",\\\"items\\\":[]}\"}}]}";
            byte[] bytes = response.getBytes(
                    java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        try {
            Map<String, String> values = configValues();
            values.put("autoLabelModelUrl", "http://127.0.0.1:"
                    + server.getAddress().getPort() + "/v1/chat/completions");
            String content = new OpenCompatibleLlmClient(
                    AutoAnnotationConfig.from(values)).complete("system", "user");

            assertEquals("Bearer test-key", authorization.get());
            assertTrue(requestBody.get().contains("\"response_format\""));
            assertTrue(content.contains("\"batchId\":\"batch-1\""));
        } finally {
            server.stop(0);
        }
    }

    private static Map<String, String> configValues() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("autoLabelEnabled", "true");
        values.put("autoLabelModelUrl", "http://127.0.0.1/v1/chat/completions");
        values.put("autoLabelApiKey", "test-key");
        values.put("autoLabelModel", "test-model");
        values.put("autoLabelMaxRowsPerBatch", "2");
        values.put("autoLabelMaxCharsPerBatch", "12000");
        values.put("autoLabelMaxColumnsPerBatch", "1");
        values.put("autoLabelMaxRetryCount", "0");
        values.put("autoLabelFailPolicy", "PARTIAL");
        return values;
    }

    private static AnnotationWorkbookData workbookData() {
        List<String> business = Arrays.asList("id", "value", "email");
        List<String> detectable = Arrays.asList("value", "email");
        List<AnnotationWorkbookRow> rows = new ArrayList<AnnotationWorkbookRow>();
        rows.add(row(2, "row-1", "1", "bad", "a@example.com"));
        rows.add(row(3, "row-2", "2", "good", "b@example.com"));
        rows.add(row(4, "row-3", "3", "good", "c@example.com"));
        return new AnnotationWorkbookData(Collections.<String, String>emptyMap(),
                business, detectable, rows);
    }

    private static AnnotationWorkbookRow row(int excelRow, String rowId,
                                             String id, String value,
                                             String email) {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("id", id);
        values.put("value", value);
        values.put("email", email);
        return new AnnotationWorkbookRow(excelRow, "task-" + rowId, rowId,
                "hash-" + rowId, values, "", "", "");
    }

    private static AutoAnnotationBatch batch(String batchId,
                                             List<AnnotationWorkbookRow> rows,
                                             List<String> columns) {
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        for (String column : columns) {
            summary.put(column, Collections.emptyMap());
        }
        return new AutoAnnotationBatch(batchId, 1, 1, columns, rows,
                summary, 1000);
    }

    private static AutoAnnotationDecision decision(String batchId,
                                                   String rowId, int label,
                                                   String errorColumn) {
        List<String> errors = errorColumn == null
                ? Collections.<String>emptyList()
                : Collections.singletonList(errorColumn);
        return new AutoAnnotationDecision(batchId, rowId, label, errors,
                0.9D, label == 1 ? "异常" : "正常");
    }

    private static List<SampleAnnotationRow> sampleRows() {
        Map<String, Object> schema = new LinkedHashMap<String, Object>();
        List<Map<String, Object>> columns = new ArrayList<Map<String, Object>>();
        columns.add(column("id", 0, false));
        columns.add(column("value", 1, true));
        columns.add(column("email", 2, true));
        schema.put("columns", columns);
        List<SampleAnnotationRow> rows = new ArrayList<SampleAnnotationRow>();
        rows.add(sample("row-1", "1", "bad", "a@example.com", schema));
        rows.add(sample("row-2", "2", "good", "b@example.com", schema));
        rows.add(sample("row-3", "3", "good", "c@example.com", schema));
        return rows;
    }

    private static Map<String, Object> column(String name, int ordinal,
                                              boolean detectable) {
        Map<String, Object> value = new LinkedHashMap<String, Object>();
        value.put("name", name);
        value.put("ordinal", Integer.valueOf(ordinal));
        value.put("dataType", "string");
        value.put("nullable", Boolean.TRUE);
        value.put("detectable", Boolean.valueOf(detectable));
        value.put("sensitive", Boolean.FALSE);
        return value;
    }

    private static SampleAnnotationRow sample(String rowId, String id,
                                              String value, String email,
                                              Map<String, Object> schema) {
        Map<String, Object> rowData = new LinkedHashMap<String, Object>();
        rowData.put("id", id);
        rowData.put("value", value);
        rowData.put("email", email);
        Map<String, Object> context = new LinkedHashMap<String, Object>();
        context.put("snapshotId", "snapshot-1");
        return new SampleAnnotationRow("task-" + rowId, rowId,
                "hash-" + rowId, "schema-1", schema, rowData, context);
    }

    private static byte[] readAll(java.io.InputStream input)
            throws java.io.IOException {
        try (java.io.InputStream stream = input;
             java.io.ByteArrayOutputStream output =
                     new java.io.ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int count;
            while ((count = stream.read(buffer)) >= 0) {
                output.write(buffer, 0, count);
            }
            return output.toByteArray();
        }
    }

    /** 测试模型根据当前字段窗口稳定返回结构化结果。 */
    private static final class DeterministicClient implements LlmClient {

        @Override
        @SuppressWarnings("unchecked")
        public String complete(String systemPrompt, String userPrompt) {
            Map<String, Object> request = FmdbJsonCodec.readObject(userPrompt);
            List<String> columns = (List<String>) request.get(
                    "detectableColumns");
            List<Map<String, Object>> items =
                    new ArrayList<Map<String, Object>>();
            for (Object value : (List<Object>) request.get("rows")) {
                Map<String, Object> row = (Map<String, Object>) value;
                Map<String, String> values = (Map<String, String>) row.get("values");
                boolean abnormal = columns.contains("value")
                        && "bad".equals(values.get("value"));
                Map<String, Object> item = new LinkedHashMap<String, Object>();
                item.put("rowId", row.get("rowId"));
                item.put("rowLabel", Integer.valueOf(abnormal ? 1 : 0));
                item.put("errorColumns", abnormal
                        ? Collections.singletonList("value")
                        : Collections.emptyList());
                item.put("confidence", Double.valueOf(0.9D));
                item.put("reason", abnormal ? "值格式异常" : "当前字段正常");
                items.add(item);
            }
            Map<String, Object> response = new LinkedHashMap<String, Object>();
            response.put("batchId", request.get("batchId"));
            response.put("items", items);
            return FmdbJsonCodec.write(response);
        }
    }

    /** 测试模型始终失败，用于验证重试和 WARN_ONLY 策略。 */
    private static final class CountingFailClient implements LlmClient {

        /** 实际调用次数。 */
        private int callCount;

        @Override
        public String complete(String systemPrompt, String userPrompt) {
            callCount++;
            throw new IllegalStateException("模拟模型不可用");
        }
    }
}
