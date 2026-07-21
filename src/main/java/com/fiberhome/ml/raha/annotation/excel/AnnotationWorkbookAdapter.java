package com.fiberhome.ml.raha.annotation.excel;

import com.fiberhome.ml.raha.annotation.domain.AnnotationImportErrorCode;
import com.fiberhome.ml.raha.annotation.domain.AnnotationRowError;
import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.sampling.domain.SampleAnnotationRow;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DataValidation;
import org.apache.poi.ss.usermodel.DataValidationConstraint;
import org.apache.poi.ss.usermodel.DataValidationHelper;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.PatternFormatting;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.SheetConditionalFormatting;
import org.apache.poi.ss.usermodel.SheetVisibility;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.CellReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用 Apache POI HSSF 导出受保护的 xls 模板，并在规模限制内读取用户标注。
 */
public final class AnnotationWorkbookAdapter {

    /** 当前 Excel 模板协议版本。 */
    public static final String TEMPLATE_VERSION = "xls-v1";
    /** 用户标注数据工作表。 */
    public static final String DATA_SHEET = "标注数据";
    /** 用户填写规则工作表。 */
    public static final String INSTRUCTION_SHEET = "标注说明";
    /** 导入错误回写工作表。 */
    public static final String VALIDATION_SHEET = "导入校验";
    /** 隐藏模板元数据工作表。 */
    public static final String SYSTEM_SHEET = "系统信息";
    /** 模板保护口令只用于防止误编辑，不作为安全边界。 */
    private static final String PROTECTION_PASSWORD = "raha-template";
    /** 固定系统字段。 */
    private static final List<String> SYSTEM_COLUMNS = Arrays.asList(
            "_annotation_task_id", "_row_id", "_row_content_hash");
    /** 固定用户标注字段。 */
    private static final List<String> ANNOTATION_COLUMNS = Arrays.asList(
            "_row_label", "_error_columns", "_comment");
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AnnotationWorkbookAdapter.class);
    /** Excel 规模和内存限制。 */
    private final AnnotationExcelConfig config;

    public AnnotationWorkbookAdapter(AnnotationExcelConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("Excel 标注配置不能为空");
        }
        this.config = config;
    }

    /**
     * 从 c1 最小投影视图生成四工作表标注模板。
     */
    public Path exportTemplate(Path outputPath,
                               String datasetId,
                               String samplePartitionMonth,
                               String sampleBatchId,
                               List<SampleAnnotationRow> rows,
                               long exportedAt) {
        if (outputPath == null || datasetId == null || sampleBatchId == null
                || samplePartitionMonth == null || rows == null || rows.isEmpty()
                || rows.size() > config.getMaximumRowCount() || exportedAt <= 0L) {
            throw new IllegalArgumentException("Excel 模板输入、行数和时间必须有效");
        }
        TemplateColumns columns = templateColumns(rows.get(0));
        validateRows(rows, columns.schemaHash);
        Path target = validateXlsPath(outputPath, "Excel 模板输出文件");
        LOGGER.info("开始导出 Excel 标注模板，datasetId={}，sampleBatchId={}，"
                        + "recordCount={}，fileName={}", datasetId, sampleBatchId,
                rows.size(), target.getFileName());
        Workbook workbook = new HSSFWorkbook();
        try {
            createDataSheet(workbook, rows, columns);
            createInstructionSheet(workbook);
            createValidationSheet(workbook);
            createSystemSheet(workbook, datasetId, samplePartitionMonth,
                    sampleBatchId, columns, rows.size(), exportedAt);
            writeWorkbook(workbook, target);
            LOGGER.info("Excel 标注模板导出完成，sampleBatchId={}，recordCount={}，"
                            + "fileName={}", sampleBatchId, rows.size(),
                    target.getFileName());
            return target;
        } catch (RuntimeException exception) {
            LOGGER.error("Excel 标注模板导出失败，sampleBatchId={}，fileName={}",
                    sampleBatchId, target.getFileName(), exception);
            throw exception;
        } finally {
            try {
                workbook.close();
            } catch (IOException exception) {
                LOGGER.warn("关闭 Excel 导出工作簿失败，fileName={}",
                        target.getFileName(), exception);
            }
        }
    }

    /**
     * 在文件大小和数据行限制内读取标注工作簿。
     */
    public AnnotationWorkbookData read(Path inputPath) {
        Path source = validateInputFile(inputPath);
        LOGGER.info("开始读取 Excel 标注文件，fileName={}，fileBytes={}",
                source.getFileName(), fileSize(source));
        try (InputStream stream = Files.newInputStream(source);
             Workbook workbook = new HSSFWorkbook(stream)) {
            Map<String, String> systemInfo = readSystemInfo(workbook);
            List<String> businessColumns = stringList(
                    systemInfo.get("businessColumnsJson"));
            List<String> detectableColumns = stringList(
                    systemInfo.get("detectableColumnsJson"));
            requiredSheet(workbook, INSTRUCTION_SHEET);
            requiredSheet(workbook, VALIDATION_SHEET);
            Sheet sheet = requiredSheet(workbook, DATA_SHEET);
            List<AnnotationWorkbookRow> rows = readRows(
                    sheet, businessColumns);
            LOGGER.info("Excel 标注文件读取完成，fileName={}，recordCount={}",
                    source.getFileName(), rows.size());
            return new AnnotationWorkbookData(systemInfo, businessColumns,
                    detectableColumns, rows);
        } catch (IOException exception) {
            LOGGER.error("读取 Excel 标注文件失败，fileName={}",
                    source.getFileName(), exception);
            throw new IllegalArgumentException("Excel 标注文件读取失败", exception);
        } catch (RuntimeException exception) {
            LOGGER.error("解析 Excel 标注文件失败，fileName={}",
                    source.getFileName(), exception);
            throw exception;
        }
    }

    /**
     * 复制上传工作簿并把无效行写入导入校验工作表。
     */
    public Path writeValidationErrors(Path inputPath,
                                      Path outputPath,
                                      List<AnnotationRowError> errors) {
        Path source = validateInputFile(inputPath);
        if (outputPath == null || errors == null || errors.isEmpty()) {
            throw new IllegalArgumentException("错误工作簿目标和错误行不能为空");
        }
        Path target = validateXlsPath(outputPath, "Excel 校验输出文件");
        byte[] workbookBytes;
        try (InputStream stream = Files.newInputStream(source);
             Workbook workbook = new HSSFWorkbook(stream);
             java.io.ByteArrayOutputStream output =
                     new java.io.ByteArrayOutputStream()) {
            Sheet sheet = workbook.getSheet(VALIDATION_SHEET);
            if (sheet == null) {
                sheet = workbook.createSheet(VALIDATION_SHEET);
            }
            clearRows(sheet);
            writeRow(sheet.createRow(0), Arrays.asList(
                    "Excel行号", "_row_id", "错误编码", "错误说明"), null);
            int rowIndex = 1;
            for (AnnotationRowError error : errors) {
                writeRow(sheet.createRow(rowIndex++), Arrays.asList(
                        String.valueOf(error.getExcelRowNumber()),
                        empty(error.getRowId()), error.getErrorCode().name(),
                        error.getMessage()), null);
            }
            workbook.write(output);
            workbookBytes = output.toByteArray();
            // 先完成输入工作簿和文件流关闭，避免输出路径与输入路径相同导致文件损坏。
            LOGGER.info("Excel 导入校验工作簿生成完成，fileName={}，errorCount={}",
                    target.getFileName(), errors.size());
        } catch (IOException exception) {
            LOGGER.error("生成 Excel 导入校验工作簿失败，fileName={}",
                    target.getFileName(), exception);
            throw new IllegalStateException("Excel 导入校验工作簿生成失败", exception);
        }
        try (OutputStream output = Files.newOutputStream(target,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            output.write(workbookBytes);
        } catch (IOException exception) {
            LOGGER.error("写入 Excel 导入校验工作簿失败，fileName={}",
                    target.getFileName(), exception);
            throw new IllegalStateException("Excel 导入校验工作簿写入失败", exception);
        }
        return target;
    }

    private static void createDataSheet(Workbook workbook,
                                        List<SampleAnnotationRow> rows,
                                        TemplateColumns columns) {
        Sheet sheet = workbook.createSheet(DATA_SHEET);
        CellStyle headerStyle = headerStyle(workbook);
        CellStyle lockedStyle = lockedStyle(workbook);
        CellStyle unlockedStyle = unlockedStyle(workbook);
        List<String> headers = new ArrayList<String>();
        headers.addAll(SYSTEM_COLUMNS);
        headers.add("_display_row_no");
        headers.addAll(ANNOTATION_COLUMNS);
        headers.addAll(columns.businessColumns);
        writeRow(sheet.createRow(0), headers, headerStyle);
        int rowIndex = 1;
        for (SampleAnnotationRow sample : rows) {
            Row row = sheet.createRow(rowIndex);
            int column = 0;
            setText(row.createCell(column++), sample.getAnnotationTaskId(), lockedStyle);
            setText(row.createCell(column++), sample.getRowId(), lockedStyle);
            setText(row.createCell(column++), sample.getRowContentHash(), lockedStyle);
            Cell display = row.createCell(column++);
            display.setCellValue(rowIndex);
            display.setCellStyle(lockedStyle);
            for (int index = 0; index < ANNOTATION_COLUMNS.size(); index++) {
                setText(row.createCell(column++), "", unlockedStyle);
            }
            for (String businessColumn : columns.businessColumns) {
                setText(row.createCell(column++), stringValue(
                        sample.getRowData().get(businessColumn)), lockedStyle);
            }
            rowIndex++;
        }
        for (int index = 0; index < SYSTEM_COLUMNS.size(); index++) {
            sheet.setColumnHidden(index, true);
        }
        int rowLabelColumn = SYSTEM_COLUMNS.size() + 1;
        addLabelValidation(sheet, rowLabelColumn, rows.size());
        addErrorConditionalFormatting(sheet, rowLabelColumn,
                headers.size() - 1, rows.size());
        sheet.createFreezePane(rowLabelColumn + ANNOTATION_COLUMNS.size(), 1);
        sheet.setAutoFilter(new CellRangeAddress(0, 0, 0, headers.size() - 1));
        setWidths(sheet, headers, columns.businessColumns.size());
        sheet.protectSheet(PROTECTION_PASSWORD);
    }

    private static void createInstructionSheet(Workbook workbook) {
        Sheet sheet = workbook.createSheet(INSTRUCTION_SHEET);
        List<List<String>> rows = Arrays.asList(
                Arrays.asList("字段", "说明"),
                Arrays.asList("_row_label", "0 表示正常，1 表示异常"),
                Arrays.asList("_error_columns", "异常行填写异常字段名，多个字段使用英文逗号分隔"),
                Arrays.asList("_comment", "可选标注说明"));
        CellStyle header = headerStyle(workbook);
        for (int index = 0; index < rows.size(); index++) {
            writeRow(sheet.createRow(index), rows.get(index), index == 0 ? header : null);
        }
        sheet.setColumnWidth(0, 24 * 256);
        sheet.setColumnWidth(1, 72 * 256);
        sheet.protectSheet(PROTECTION_PASSWORD);
    }

    private static void createValidationSheet(Workbook workbook) {
        Sheet sheet = workbook.createSheet(VALIDATION_SHEET);
        writeRow(sheet.createRow(0), Arrays.asList(
                "Excel行号", "_row_id", "错误编码", "错误说明"),
                headerStyle(workbook));
        sheet.createFreezePane(0, 1);
    }

    private static void createSystemSheet(Workbook workbook,
                                          String datasetId,
                                          String samplePartitionMonth,
                                          String sampleBatchId,
                                          TemplateColumns columns,
                                          int recordCount,
                                          long exportedAt) {
        Sheet sheet = workbook.createSheet(SYSTEM_SHEET);
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("templateVersion", TEMPLATE_VERSION);
        values.put("datasetId", datasetId);
        values.put("samplePartitionMonth", samplePartitionMonth);
        values.put("sampleBatchId", sampleBatchId);
        values.put("schemaHash", columns.schemaHash);
        values.put("exportedAt", String.valueOf(exportedAt));
        values.put("recordCount", String.valueOf(recordCount));
        values.put("businessColumnsJson", listJson(columns.businessColumns));
        values.put("detectableColumnsJson", listJson(columns.detectableColumns));
        int rowIndex = 0;
        for (Map.Entry<String, String> entry : values.entrySet()) {
            writeRow(sheet.createRow(rowIndex++), Arrays.asList(
                    entry.getKey(), entry.getValue()), null);
        }
        workbook.setSheetVisibility(workbook.getSheetIndex(sheet),
                SheetVisibility.VERY_HIDDEN);
    }

    @SuppressWarnings("unchecked")
    private static TemplateColumns templateColumns(SampleAnnotationRow row) {
        Object values = row.getColumnSchema().get("columns");
        if (!(values instanceof List)) {
            throw new IllegalArgumentException("c1 字段模式缺少 columns");
        }
        List<Map<String, Object>> definitions =
                new ArrayList<Map<String, Object>>((List<Map<String, Object>>) values);
        Collections.sort(definitions, (first, second) -> Integer.compare(
                ((Number) first.get("ordinal")).intValue(),
                ((Number) second.get("ordinal")).intValue()));
        List<String> business = new ArrayList<String>();
        List<String> detectable = new ArrayList<String>();
        for (Map<String, Object> definition : definitions) {
            String name = String.valueOf(definition.get("name"));
            business.add(name);
            if (Boolean.TRUE.equals(definition.get("detectable"))) {
                detectable.add(name);
            }
        }
        if (business.isEmpty() || detectable.isEmpty()) {
            throw new IllegalArgumentException("c1 模式没有业务字段或可检测字段");
        }
        return new TemplateColumns(row.getSchemaHash(), business, detectable);
    }

    private static void validateRows(List<SampleAnnotationRow> rows,
                                     String schemaHash) {
        for (SampleAnnotationRow row : rows) {
            if (row == null || !schemaHash.equals(row.getSchemaHash())) {
                throw new IllegalArgumentException("c1 标注行模式不一致");
            }
        }
    }

    private List<AnnotationWorkbookRow> readRows(
            Sheet sheet,
            List<String> businessColumns) {
        Map<String, Integer> indexes = headerIndexes(sheet.getRow(0));
        List<String> required = new ArrayList<String>();
        required.addAll(SYSTEM_COLUMNS);
        required.add("_display_row_no");
        required.addAll(ANNOTATION_COLUMNS);
        required.addAll(businessColumns);
        for (String column : required) {
            if (!indexes.containsKey(column)) {
                throw new IllegalArgumentException("标注数据缺少字段：" + column);
            }
        }
        DataFormatter formatter = new DataFormatter(Locale.ROOT);
        List<AnnotationWorkbookRow> result = new ArrayList<AnnotationWorkbookRow>();
        for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            if (row == null) {
                continue;
            }
            String rowId = text(row, indexes.get("_row_id"), formatter);
            String rowLabel = text(row, indexes.get("_row_label"), formatter);
            if (rowId.isEmpty() && rowLabel.isEmpty() && emptyRow(row, formatter)) {
                continue;
            }
            if (result.size() >= config.getMaximumRowCount()) {
                throw new IllegalArgumentException("标注数据行数超过配置上限");
            }
            Map<String, String> businessValues =
                    new LinkedHashMap<String, String>();
            for (String column : businessColumns) {
                // 业务值必须保留原始空格，导入时才能可靠识别用户对内容的修改。
                businessValues.put(column, rawText(row, indexes.get(column), formatter));
            }
            result.add(new AnnotationWorkbookRow(rowIndex + 1,
                    text(row, indexes.get("_annotation_task_id"), formatter),
                    rowId, text(row, indexes.get("_row_content_hash"), formatter),
                    businessValues, rowLabel,
                    text(row, indexes.get("_error_columns"), formatter),
                    text(row, indexes.get("_comment"), formatter)));
        }
        return result;
    }

    private static Map<String, String> readSystemInfo(Workbook workbook) {
        Sheet sheet = requiredSheet(workbook, SYSTEM_SHEET);
        DataFormatter formatter = new DataFormatter(Locale.ROOT);
        Map<String, String> result = new LinkedHashMap<String, String>();
        for (int index = 0; index <= sheet.getLastRowNum(); index++) {
            Row row = sheet.getRow(index);
            if (row == null) {
                continue;
            }
            String key = text(row, 0, formatter);
            if (!key.isEmpty()) {
                result.put(key, text(row, 1, formatter));
            }
        }
        return result;
    }

    private Path validateInputFile(Path inputPath) {
        if (inputPath == null) {
            throw new IllegalArgumentException("Excel 标注文件不能为空");
        }
        Path source = validateXlsPath(inputPath, "Excel 标注文件");
        long size = fileSize(source);
        if (size <= 0L || size > config.getMaximumFileBytes()) {
            throw new IllegalArgumentException("Excel 标注文件大小超出限制");
        }
        return source;
    }

    private static Path validateXlsPath(Path path, String name) {
        if (path == null) {
            throw new IllegalArgumentException(name + "不能为空");
        }
        Path normalized = path.toAbsolutePath().normalize();
        String fileName = normalized.getFileName() == null ? ""
                : normalized.getFileName().toString().toLowerCase(Locale.ROOT);
        if (!fileName.endsWith(".xls")) {
            throw new IllegalArgumentException(name + "必须使用 .xls 扩展名");
        }
        return normalized;
    }

    private static long fileSize(Path source) {
        try {
            return Files.size(source);
        } catch (IOException exception) {
            throw new IllegalArgumentException("无法读取 Excel 标注文件大小", exception);
        }
    }

    private static void writeWorkbook(Workbook workbook, Path target) {
        try {
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (OutputStream stream = Files.newOutputStream(target,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                workbook.write(stream);
            }
        } catch (IOException exception) {
            throw new IllegalStateException("写入 Excel 工作簿失败", exception);
        }
    }

    private static CellStyle headerStyle(Workbook workbook) {
        CellStyle style = bordered(workbook);
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setAlignment(HorizontalAlignment.CENTER);
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        style.setLocked(true);
        return style;
    }

    private static CellStyle lockedStyle(Workbook workbook) {
        CellStyle style = bordered(workbook);
        style.setLocked(true);
        return style;
    }

    private static CellStyle unlockedStyle(Workbook workbook) {
        CellStyle style = bordered(workbook);
        style.setLocked(false);
        style.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        DataFormat format = workbook.createDataFormat();
        style.setDataFormat(format.getFormat("@"));
        return style;
    }

    private static CellStyle bordered(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setBorderTop(BorderStyle.THIN);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBorderLeft(BorderStyle.THIN);
        style.setBorderRight(BorderStyle.THIN);
        return style;
    }

    private static void addLabelValidation(Sheet sheet,
                                           int columnIndex,
                                           int rowCount) {
        DataValidationHelper helper = sheet.getDataValidationHelper();
        DataValidationConstraint constraint = helper.createExplicitListConstraint(
                new String[] {"0", "1"});
        DataValidation validation = helper.createValidation(constraint,
                new CellRangeAddressList(1, Math.max(1, rowCount),
                        columnIndex, columnIndex));
        validation.setShowErrorBox(true);
        validation.createErrorBox("标签无效", "只能填写 0 或 1");
        sheet.addValidationData(validation);
    }

    private static void addErrorConditionalFormatting(Sheet sheet,
                                                      int rowLabelColumn,
                                                      int lastColumn,
                                                      int rowCount) {
        SheetConditionalFormatting formatting = sheet.getSheetConditionalFormatting();
        String column = CellReference.convertNumToColString(rowLabelColumn);
        org.apache.poi.ss.usermodel.ConditionalFormattingRule rule =
                formatting.createConditionalFormattingRule("$" + column + "2=1");
        PatternFormatting pattern = rule.createPatternFormatting();
        pattern.setFillBackgroundColor(IndexedColors.ROSE.getIndex());
        pattern.setFillPattern(PatternFormatting.SOLID_FOREGROUND);
        formatting.addConditionalFormatting(new CellRangeAddress[] {
                new CellRangeAddress(1, Math.max(1, rowCount), 0, lastColumn)}, rule);
    }

    private static void setWidths(Sheet sheet,
                                  List<String> headers,
                                  int businessColumnCount) {
        int businessStart = SYSTEM_COLUMNS.size() + 1 + ANNOTATION_COLUMNS.size();
        int businessEnd = businessStart + businessColumnCount;
        for (int index = 0; index < headers.size(); index++) {
            int width = index >= businessStart && index < businessEnd ? 24 : 18;
            sheet.setColumnWidth(index, width * 256);
        }
        int commentColumn = headers.indexOf("_comment");
        if (commentColumn >= 0) {
            sheet.setColumnWidth(commentColumn, 36 * 256);
        }
    }

    private static Map<String, Integer> headerIndexes(Row row) {
        if (row == null) {
            throw new IllegalArgumentException("标注数据缺少表头");
        }
        DataFormatter formatter = new DataFormatter(Locale.ROOT);
        Map<String, Integer> indexes = new LinkedHashMap<String, Integer>();
        for (Cell cell : row) {
            String value = formatter.formatCellValue(cell).trim();
            if (!value.isEmpty() && indexes.put(value, cell.getColumnIndex()) != null) {
                throw new IllegalArgumentException("标注数据表头字段重复：" + value);
            }
        }
        return indexes;
    }

    private static Sheet requiredSheet(Workbook workbook, String name) {
        Sheet sheet = workbook.getSheet(name);
        if (sheet == null) {
            throw new IllegalArgumentException("Excel 标注文件缺少工作表：" + name);
        }
        return sheet;
    }

    private static void writeRow(Row row, List<String> values, CellStyle style) {
        for (int index = 0; index < values.size(); index++) {
            setText(row.createCell(index), values.get(index), style);
        }
    }

    private static void setText(Cell cell, String value, CellStyle style) {
        cell.setCellValue(empty(value));
        if (style != null) {
            cell.setCellStyle(style);
        }
    }

    private static String text(Row row, int index, DataFormatter formatter) {
        Cell cell = row.getCell(index, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
        return cell == null ? "" : formatter.formatCellValue(cell).trim();
    }

    private static String rawText(Row row, int index, DataFormatter formatter) {
        Cell cell = row.getCell(index, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
        return cell == null ? "" : formatter.formatCellValue(cell);
    }

    private static boolean emptyRow(Row row, DataFormatter formatter) {
        for (Cell cell : row) {
            if (!formatter.formatCellValue(cell).trim().isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static void clearRows(Sheet sheet) {
        for (int index = sheet.getLastRowNum(); index >= 0; index--) {
            Row row = sheet.getRow(index);
            if (row != null) {
                sheet.removeRow(row);
            }
        }
    }

    private static String listJson(List<String> values) {
        Map<String, Object> object = new LinkedHashMap<String, Object>();
        object.put("columns", values);
        return FmdbJsonCodec.write(object);
    }

    @SuppressWarnings("unchecked")
    private static List<String> stringList(String json) {
        Object values = FmdbJsonCodec.readObject(json).get("columns");
        if (!(values instanceof List)) {
            throw new IllegalArgumentException("Excel 系统信息字段列表无效");
        }
        List<String> result = new ArrayList<String>();
        for (Object value : (List<Object>) values) {
            result.add(String.valueOf(value));
        }
        return result;
    }

    private static String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static String empty(String value) {
        return value == null ? "" : value;
    }

    /** 有序业务字段、可检测字段和模式哈希。 */
    private static final class TemplateColumns {

        /** c1 模式哈希。 */
        private final String schemaHash;
        /** 有序业务字段。 */
        private final List<String> businessColumns;
        /** 有序可检测字段。 */
        private final List<String> detectableColumns;

        private TemplateColumns(String schemaHash,
                                List<String> businessColumns,
                                List<String> detectableColumns) {
            this.schemaHash = schemaHash;
            this.businessColumns = businessColumns;
            this.detectableColumns = detectableColumns;
        }
    }
}
