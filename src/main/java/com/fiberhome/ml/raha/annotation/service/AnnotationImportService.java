package com.fiberhome.ml.raha.annotation.service;

import com.fiberhome.ml.raha.annotation.domain.AnnotationBatch;
import com.fiberhome.ml.raha.annotation.domain.AnnotationBatchStatus;
import com.fiberhome.ml.raha.annotation.domain.AnnotationImportErrorCode;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRecord;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRowError;
import com.fiberhome.ml.raha.annotation.domain.RowAnnotation;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookData;
import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookRow;
import com.fiberhome.ml.raha.repository.adapter.fmdb.schema.FmdbPartitionUtils;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.repository.port.AnnotationRecordRepository;
import com.fiberhome.ml.raha.repository.port.SampleRecordRepository;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 负责 Excel 标注导入的完整校验、可信快照回填、标签展开和批次追加。
 */
public final class AnnotationImportService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AnnotationImportService.class);
    /** c1 采样记录仓储。 */
    private final SampleRecordRepository sampleRepository;
    /** 标注批次追加仓储。 */
    private final AnnotationRecordRepository annotationRepository;
    /** Excel 工作簿适配器。 */
    private final AnnotationWorkbookAdapter workbookAdapter;
    /** 整行标签展开器。 */
    private final AnnotationLabelExpander labelExpander;
    /** 提供可测试导入时间的时钟。 */
    private final Clock clock;

    public AnnotationImportService(SampleRecordRepository sampleRepository,
                                   AnnotationRecordRepository annotationRepository,
                                   AnnotationWorkbookAdapter workbookAdapter,
                                   AnnotationLabelExpander labelExpander,
                                   Clock clock) {
        if (sampleRepository == null || annotationRepository == null
                || workbookAdapter == null || labelExpander == null
                || clock == null) {
            throw new IllegalArgumentException("标注导入服务依赖不能为空");
        }
        this.sampleRepository = sampleRepository;
        this.annotationRepository = annotationRepository;
        this.workbookAdapter = workbookAdapter;
        this.labelExpander = labelExpander;
        this.clock = clock;
    }

    /**
     * 读取并导入一个离线标注工作簿。
     *
     * @param request 导入请求
     * @return 导入状态、有效标签和错误工作簿
     */
    public AnnotationImportResult importWorkbook(AnnotationImportRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("标注导入请求不能为空");
        }
        long startedAt = clock.millis();
        if (startedAt <= 0L) {
            throw new IllegalStateException("标注导入时钟必须返回正时间");
        }
        LOGGER.info("开始导入 Excel 标注，datasetId={}，sampleBatchId={}，fileName={}",
                request.getDatasetId(), request.getSampleBatchId(),
                request.getInputPath().getFileName());

        String fingerprint = fileFingerprint(request.getInputPath());
        if (annotationRepository.existsImportFingerprint(request.getDatasetId(),
                request.getSampleBatchId(), fingerprint)) {
            LOGGER.warn("检测到重复标注文件，datasetId={}，sampleBatchId={}，"
                            + "fingerprint={}", request.getDatasetId(),
                    request.getSampleBatchId(), fingerprint);
            return new AnnotationImportResult(AnnotationBatchStatus.DUPLICATE,
                    null, Collections.<com.fiberhome.ml.raha.label.CellLabel>emptyList(),
                    Collections.<AnnotationRowError>emptyList(), null);
        }

        AnnotationWorkbookData workbook;
        try {
            workbook = workbookAdapter.read(request.getInputPath());
        } catch (RuntimeException exception) {
            LOGGER.error("标注工作簿结构或文件读取失败，fileName={}",
                    request.getInputPath().getFileName(), exception);
            List<AnnotationRowError> errors = Collections.singletonList(
                    new AnnotationRowError(1, null,
                            AnnotationImportErrorCode.WORKBOOK_INVALID,
                            "工作簿结构或文件内容无效"));
            Path validation = writeErrors(request, errors);
            return new AnnotationImportResult(AnnotationBatchStatus.REJECTED,
                    null, Collections.<com.fiberhome.ml.raha.label.CellLabel>emptyList(),
                    errors, validation);
        }

        List<SampleAnnotationRow> samples = sampleRepository.findForAnnotation(
                request.getDatasetId(), request.getSamplePartitionMonth(),
                request.getSampleBatchId());
        if (samples == null || samples.isEmpty()) {
            LOGGER.warn("指定 c1 批次不存在，datasetId={}，sampleBatchId={}",
                    request.getDatasetId(), request.getSampleBatchId());
            List<AnnotationRowError> errors = Collections.singletonList(
                    new AnnotationRowError(1, null,
                            AnnotationImportErrorCode.ROW_NOT_IN_SAMPLE,
                            "指定采样批次不存在或没有可标注记录"));
            Path validation = writeErrors(request, errors);
            return new AnnotationImportResult(AnnotationBatchStatus.REJECTED,
                    null, Collections.<com.fiberhome.ml.raha.label.CellLabel>emptyList(),
                    errors, validation);
        }

        Map<String, SampleAnnotationRow> sampleByRow = indexSamples(samples);
        List<AnnotationRowError> errors = new ArrayList<AnnotationRowError>();
        validateWorkbookMetadata(request, workbook, samples.get(0), errors);
        if (!errors.isEmpty()) {
            Path validation = writeErrors(request, errors);
            return new AnnotationImportResult(AnnotationBatchStatus.REJECTED,
                    null, Collections.<com.fiberhome.ml.raha.label.CellLabel>emptyList(),
                    errors, validation);
        }

        String annotationBatchId = "ann-" + HashUtils.sha256Hex(
                request.getDatasetId() + "|" + request.getSampleBatchId()
                        + "|" + fingerprint);
        validateRevision(request, annotationBatchId, errors);
        if (!errors.isEmpty()) {
            Path validation = writeErrors(request, errors);
            return new AnnotationImportResult(AnnotationBatchStatus.REJECTED,
                    null, Collections.<com.fiberhome.ml.raha.label.CellLabel>emptyList(),
                    errors, validation);
        }

        List<PendingAnnotation> pendingRecords = new ArrayList<PendingAnnotation>();
        List<com.fiberhome.ml.raha.label.CellLabel> labels =
                new ArrayList<com.fiberhome.ml.raha.label.CellLabel>();
        Set<String> seenRows = new LinkedHashSet<String>();
        int validCount = 0;
        int invalidCount = 0;
        for (AnnotationWorkbookRow row : workbook.getRows()) {
            List<AnnotationRowError> rowErrors = validateRow(request, workbook,
                    row, sampleByRow, seenRows);
            if (!rowErrors.isEmpty()) {
                errors.addAll(rowErrors);
                invalidCount++;
                continue;
            }
            SampleAnnotationRow sample = sampleByRow.get(row.getRowId());
            Set<String> errorColumns = parseColumns(row.getErrorColumns());
            int rowLabel = Integer.parseInt(row.getRowLabel().trim());
            String snapshotId = contextText(sample.getSamplingContext(), "snapshotId");
            List<String> reviewedColumns = expectedDetectableColumns(
                    sample.getColumnSchema());
            List<com.fiberhome.ml.raha.label.CellLabel> rowLabels =
                    labelExpander.expand(request.getDatasetId(), snapshotId,
                            row.getRowId(), rowLabel, reviewedColumns,
                            errorColumns, request.getAnnotator(), startedAt,
                            annotationBatchId);
            RowAnnotation annotation = new RowAnnotation(
                    blankToNull(row.getAnnotationTaskId()), row.getRowId(),
                    snapshotId, row.getRowContentHash(), rowLabel,
                    new LinkedHashSet<String>(reviewedColumns), errorColumns,
                    blankToNull(row.getComment()), rowLabels);
            pendingRecords.add(new PendingAnnotation(annotation, sample.getRowData(),
                    sample.getSchemaHash()));
            labels.addAll(rowLabels);
            validCount++;
        }

        if (validCount == 0) {
            LOGGER.warn("标注文件没有有效行，datasetId={}，sampleBatchId={}，"
                            + "invalidRowCount={}", request.getDatasetId(),
                    request.getSampleBatchId(), invalidCount);
            Path validation = writeErrors(request, errors);
            return new AnnotationImportResult(AnnotationBatchStatus.REJECTED,
                    null, Collections.<com.fiberhome.ml.raha.label.CellLabel>emptyList(),
                    errors, validation);
        }

        AnnotationBatchStatus status = errors.isEmpty()
                ? AnnotationBatchStatus.IMPORTED : AnnotationBatchStatus.PARTIAL;
        List<AnnotationRecord> normalizedRecords = buildRecords(pendingRecords,
                annotationBatchId, request, workbook.getRows().size(), validCount,
                invalidCount, status, startedAt, fingerprint);
        AnnotationBatch batch = new AnnotationBatch(annotationBatchId,
                request.getSampleBatchId(), request.getDatasetId(), status,
                startedAt, FmdbPartitionUtils.month(startedAt),
                request.getSupersedesBatchId(), normalizedRecords);
        if (!annotationRepository.isPersistenceEnabled()) {
            throw new IllegalStateException("标注记录持久化开关已关闭，不能报告导入成功");
        }
        try {
            long written = annotationRepository.saveAll(batch);
            if (written != normalizedRecords.size()) {
                throw new IllegalStateException("标注批次物理写入数量不一致");
            }
            Path validation = writeErrors(request, errors);
            LOGGER.info("标注批次导入完成，annotationBatchId={}，status={}，"
                            + "validCount={}，invalidCount={}，writtenCount={}",
                    annotationBatchId, status, validCount, invalidCount, written);
            return new AnnotationImportResult(status, batch, labels, errors, validation);
        } catch (RuntimeException exception) {
            LOGGER.error("标注批次物理追加失败，annotationBatchId={}，validCount={}",
                    annotationBatchId, validCount, exception);
            throw exception;
        }
    }

    /** 为调用方提供语义更明确的别名。 */
    public AnnotationImportResult importAnnotations(AnnotationImportRequest request) {
        return importWorkbook(request);
    }

    private void validateWorkbookMetadata(AnnotationImportRequest request,
                                          AnnotationWorkbookData workbook,
                                          SampleAnnotationRow sample,
                                          List<AnnotationRowError> errors) {
        Map<String, String> info = workbook.getSystemInfo();
        if (!AnnotationWorkbookAdapter.TEMPLATE_VERSION.equals(
                info.get("templateVersion"))) {
            errors.add(workbookError(AnnotationImportErrorCode.TEMPLATE_VERSION_INVALID,
                    "模板版本不受支持"));
        }
        if (!request.getDatasetId().equals(info.get("datasetId"))
                || !request.getSamplePartitionMonth().equals(
                info.get("samplePartitionMonth"))
                || !request.getSampleBatchId().equals(info.get("sampleBatchId"))) {
            errors.add(workbookError(AnnotationImportErrorCode.SAMPLE_BATCH_MISMATCH,
                    "模板数据集、月分区或采样批次不匹配"));
        }
        if (!sample.getSchemaHash().equals(info.get("schemaHash"))) {
            errors.add(workbookError(AnnotationImportErrorCode.SCHEMA_HASH_MISMATCH,
                    "模板模式哈希与采样批次不匹配"));
        }
        List<String> expectedBusiness = expectedBusinessColumns(
                sample.getColumnSchema());
        List<String> expectedDetectable = expectedDetectableColumns(
                sample.getColumnSchema());
        if (!expectedBusiness.equals(workbook.getBusinessColumns())
                || !expectedDetectable.equals(workbook.getDetectableColumns())) {
            errors.add(workbookError(AnnotationImportErrorCode.WORKBOOK_INVALID,
                    "模板业务字段或可检测字段顺序不匹配"));
        }
        String count = info.get("recordCount");
        if (count == null || !String.valueOf(workbook.getRows().size()).equals(count)) {
            errors.add(workbookError(AnnotationImportErrorCode.WORKBOOK_INVALID,
                    "模板记录数量与数据行不匹配"));
        }
    }

    private List<AnnotationRowError> validateRow(
            AnnotationImportRequest request,
            AnnotationWorkbookData workbook,
            AnnotationWorkbookRow row,
            Map<String, SampleAnnotationRow> sampleByRow,
            Set<String> seenRows) {
        List<AnnotationRowError> errors = new ArrayList<AnnotationRowError>();
        String rowId = blankToNull(row.getRowId());
        if (rowId == null) {
            errors.add(rowError(row, null, AnnotationImportErrorCode.ROW_ID_MISSING,
                    "系统行标识不能为空"));
            return errors;
        }
        if (!seenRows.add(rowId)) {
            errors.add(rowError(row, rowId, AnnotationImportErrorCode.DUPLICATE_ROW,
                    "同一逻辑行在文件中重复出现"));
        }
        SampleAnnotationRow sample = sampleByRow.get(rowId);
        if (sample == null) {
            errors.add(rowError(row, rowId, AnnotationImportErrorCode.ROW_NOT_IN_SAMPLE,
                    "逻辑行不属于指定采样批次"));
            return errors;
        }
        String taskId = blankToNull(row.getAnnotationTaskId());
        if ((!request.isAllowBlankTaskId() && taskId == null)
                || (taskId != null && !taskId.equals(sample.getAnnotationTaskId()))) {
            errors.add(rowError(row, rowId,
                    AnnotationImportErrorCode.ANNOTATION_TASK_MISMATCH,
                    "标注任务标识不属于当前采样行"));
        }
        if (!sample.getRowContentHash().equals(row.getRowContentHash())) {
            errors.add(rowError(row, rowId,
                    AnnotationImportErrorCode.ROW_CONTENT_HASH_MISMATCH,
                    "行内容哈希与可信采样快照不一致"));
        }
        if (!sameBusinessValues(row.getBusinessValues(), sample.getRowData(),
                expectedBusinessColumns(sample.getColumnSchema()))) {
            errors.add(rowError(row, rowId,
                    AnnotationImportErrorCode.BUSINESS_DATA_CHANGED,
                    "业务字段值与可信采样快照不一致"));
        }
        String labelText = blankToNull(row.getRowLabel());
        int rowLabel = -1;
        if (labelText == null || !("0".equals(labelText) || "1".equals(labelText))) {
            errors.add(rowError(row, rowId, AnnotationImportErrorCode.ROW_LABEL_INVALID,
                    "整行标签只能填写 0 或 1"));
        } else {
            rowLabel = Integer.parseInt(labelText);
        }
        Set<String> errorColumns = parseColumns(row.getErrorColumns());
        Set<String> detectable = new LinkedHashSet<String>(
                expectedDetectableColumns(sample.getColumnSchema()));
        if (!detectable.containsAll(errorColumns)) {
            errors.add(rowError(row, rowId,
                    AnnotationImportErrorCode.ERROR_COLUMN_INVALID,
                    "异常字段包含不存在或不可检测字段"));
        }
        if (rowLabel == 0 && !errorColumns.isEmpty()) {
            errors.add(rowError(row, rowId,
                    AnnotationImportErrorCode.ROW_LABEL_INVALID,
                    "正常行不能填写异常字段"));
        }
        if (rowLabel == 1 && errorColumns.isEmpty()) {
            errors.add(rowError(row, rowId,
                    AnnotationImportErrorCode.ROW_LABEL_INVALID,
                    "异常行至少需要填写一个异常字段"));
        }
        return errors;
    }

    private void validateRevision(AnnotationImportRequest request,
                                  String annotationBatchId,
                                  List<AnnotationRowError> errors) {
        if (request.getSupersedesBatchId() == null) {
            return;
        }
        if (annotationBatchId.equals(request.getSupersedesBatchId())) {
            errors.add(workbookError(AnnotationImportErrorCode.REVISION_INVALID,
                    "修订批次不能引用自身"));
            return;
        }
        java.util.Optional<AnnotationBatch> previous = annotationRepository.find(
                request.getDatasetId(), request.getSupersedesPartitionMonth(),
                request.getSupersedesBatchId());
        if (!previous.isPresent()
                || !request.getDatasetId().equals(previous.get().getDatasetId())
                || !request.getSampleBatchId().equals(previous.get().getSampleBatchId())) {
            errors.add(workbookError(AnnotationImportErrorCode.REVISION_INVALID,
                    "被修订批次不存在、跨数据集或不属于同一采样批次"));
        }
    }

    private static List<AnnotationRecord> buildRecords(
            List<PendingAnnotation> pending,
            String annotationBatchId,
            AnnotationImportRequest request,
            int batchCount,
            int validCount,
            int invalidCount,
            AnnotationBatchStatus status,
            long annotatedAt,
            String fingerprint) {
        List<AnnotationRecord> records = new ArrayList<AnnotationRecord>(pending.size());
        for (PendingAnnotation item : pending) {
            records.add(new AnnotationRecord(annotationBatchId,
                    request.getSampleBatchId(), request.getDatasetId(),
                    item.annotation, item.rowData,
                    AnnotationWorkbookAdapter.TEMPLATE_VERSION,
                    request.getInputPath().getFileName().toString(),
                    // 模式哈希已经在批次级校验，这里从标签行的 c1 快照读取。
                    item.schemaHash, request.getAnnotator(), status,
                    batchCount, validCount, invalidCount,
                    request.getSupersedesBatchId(), annotatedAt,
                    FmdbPartitionUtils.month(annotatedAt), fingerprint));
        }
        return records;
    }

    private Path writeErrors(AnnotationImportRequest request,
                             List<AnnotationRowError> errors) {
        if (errors == null || errors.isEmpty()
                || request.getValidationOutputPath() == null) {
            return null;
        }
        try {
            return workbookAdapter.writeValidationErrors(request.getInputPath(),
                    request.getValidationOutputPath(), errors);
        } catch (RuntimeException exception) {
            // 原文件无法解析时不能生成错误副本，但必须保留原始拒绝结果。
            LOGGER.error("标注错误工作簿生成失败，fileName={}，errorCount={}",
                    request.getInputPath().getFileName(), errors.size(), exception);
            return null;
        }
    }

    private static Map<String, SampleAnnotationRow> indexSamples(
            List<SampleAnnotationRow> samples) {
        Map<String, SampleAnnotationRow> result =
                new LinkedHashMap<String, SampleAnnotationRow>();
        for (SampleAnnotationRow sample : samples) {
            if (sample == null || result.put(sample.getRowId(), sample) != null) {
                throw new IllegalStateException("c1 采样批次存在重复逻辑行");
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<String> expectedBusinessColumns(Map<String, Object> schema) {
        Object raw = schema.get("columns");
        if (!(raw instanceof List)) {
            throw new IllegalArgumentException("c1 字段模式缺少 columns");
        }
        List<Map<String, Object>> definitions =
                new ArrayList<Map<String, Object>>((List<Map<String, Object>>) raw);
        Collections.sort(definitions, (left, right) -> Integer.compare(
                ((Number) left.get("ordinal")).intValue(),
                ((Number) right.get("ordinal")).intValue()));
        List<String> result = new ArrayList<String>(definitions.size());
        for (Map<String, Object> definition : definitions) {
            result.add(String.valueOf(definition.get("name")));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static List<String> expectedDetectableColumns(Map<String, Object> schema) {
        Object raw = schema.get("columns");
        if (!(raw instanceof List)) {
            throw new IllegalArgumentException("c1 字段模式缺少 columns");
        }
        List<Map<String, Object>> definitions =
                new ArrayList<Map<String, Object>>((List<Map<String, Object>>) raw);
        Collections.sort(definitions, (left, right) -> Integer.compare(
                ((Number) left.get("ordinal")).intValue(),
                ((Number) right.get("ordinal")).intValue()));
        List<String> result = new ArrayList<String>();
        for (Map<String, Object> definition : definitions) {
            if (Boolean.TRUE.equals(definition.get("detectable"))) {
                result.add(String.valueOf(definition.get("name")));
            }
        }
        return result;
    }

    private static boolean sameBusinessValues(Map<String, String> uploaded,
                                              Map<String, Object> trusted,
                                              List<String> columns) {
        if (uploaded == null || trusted == null
                || uploaded.size() != columns.size()) {
            return false;
        }
        for (String column : columns) {
            if (!uploaded.containsKey(column)
                    || !canonicalText(uploaded.get(column)).equals(
                    canonicalText(trusted.get(column)))) {
                return false;
            }
        }
        return true;
    }

    private static String canonicalText(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof Number) {
            try {
                return new BigDecimal(String.valueOf(value))
                        .stripTrailingZeros().toPlainString();
            } catch (NumberFormatException ignored) {
                return String.valueOf(value);
            }
        }
        if (value instanceof Map || value instanceof List) {
            return FmdbJsonCodec.write(value);
        }
        return String.valueOf(value);
    }

    private static Set<String> parseColumns(String text) {
        String source = text == null ? "" : text.trim();
        if (source.isEmpty()) {
            return new LinkedHashSet<String>();
        }
        Set<String> result = new LinkedHashSet<String>();
        for (String value : source.split("[,，]")) {
            String column = value.trim();
            if (!column.isEmpty()) {
                result.add(column);
            }
        }
        return result;
    }

    private static String contextText(Map<String, Object> context, String key) {
        Object value = context.get(key);
        return ValueUtils.requireNotBlank(value == null ? null : String.valueOf(value),
                "采样上下文字段" + key);
    }

    private static String blankToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static AnnotationRowError rowError(AnnotationWorkbookRow row,
                                               String rowId,
                                               AnnotationImportErrorCode code,
                                               String message) {
        return new AnnotationRowError(row.getExcelRowNumber(), rowId, code, message);
    }

    private static AnnotationRowError workbookError(
            AnnotationImportErrorCode code, String message) {
        return new AnnotationRowError(1, null, code, message);
    }

    private static String fileFingerprint(Path inputPath) {
        try (InputStream stream = Files.newInputStream(inputPath)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int read;
            while ((read = stream.read(buffer)) >= 0) {
                if (read > 0) {
                    digest.update(buffer, 0, read);
                }
            }
            StringBuilder value = new StringBuilder(64);
            for (byte current : digest.digest()) {
                value.append(String.format(Locale.ROOT, "%02x", current & 0xff));
            }
            return value.toString();
        } catch (IOException exception) {
            throw new IllegalArgumentException("无法读取标注文件指纹", exception);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("当前 Java 环境不支持 SHA-256", exception);
        }
    }

    /** 保存行展开结果，待全部行校验结束后补齐批次统计字段。 */
    private static final class PendingAnnotation {

        /** 整行标注和直接标签。 */
        private final RowAnnotation annotation;
        /** 可信 c1 原始行。 */
        private final Map<String, Object> rowData;
        /** c1 模式哈希。 */
        private final String schemaHash;

        private PendingAnnotation(RowAnnotation annotation,
                                  Map<String, Object> rowData,
                                  String schemaHash) {
            this.annotation = annotation;
            this.rowData = rowData;
            this.schemaHash = schemaHash;
        }
    }
}
