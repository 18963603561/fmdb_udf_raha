package com.fiberhome.ml.raha.annotation.auto;

import com.fiberhome.ml.raha.annotation.excel.AnnotationWorkbookAdapter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 复制原始模板并仅回写标签、异常字段和说明，同时增加自动标注明细表。
 */
public final class AnnotationAutoLabelWorkbookWriter {

    /** 自动标注明细工作表名称。 */
    public static final String DETAIL_SHEET = "自动标注明细";
    /** Excel 单元格文本安全上限。 */
    private static final int MAX_CELL_CHARS = 32000;
    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            AnnotationAutoLabelWorkbookWriter.class);

    /**
     * 生成自动标注工作簿，不修改输入文件。
     *
     * @param input 原始标注模板
     * @param output 自动标注输出模板
     * @param decisions 合并后的行级决策
     * @return 输出文件
     */
    public Path write(Path input, Path output,
                      List<AutoAnnotationDecision> decisions) {
        if (input == null || output == null || decisions == null) {
            throw new IllegalArgumentException("自动标注工作簿参数不能为空");
        }
        Path source = input.toAbsolutePath().normalize();
        Path target = output.toAbsolutePath().normalize();
        LOGGER.info("开始回写自动标注 Excel，sourceFile={}，targetFile={}，decisionCount={}",
                source.getFileName(), target.getFileName(), decisions.size());
        byte[] bytes;
        try (InputStream stream = Files.newInputStream(source);
             Workbook workbook = new HSSFWorkbook(stream);
             ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.getSheet(AnnotationWorkbookAdapter.DATA_SHEET);
            if (sheet == null || sheet.getRow(0) == null) {
                throw new IllegalArgumentException("标注工作簿缺少标注数据表头");
            }
            Map<String, Integer> indexes = indexes(sheet.getRow(0));
            int rowIdColumn = required(indexes, "_row_id");
            int labelColumn = required(indexes, "_row_label");
            int errorColumn = required(indexes, "_error_columns");
            int commentColumn = required(indexes, "_comment");
            Map<String, AutoAnnotationDecision> byRow =
                    new HashMap<String, AutoAnnotationDecision>();
            for (AutoAnnotationDecision decision : decisions) {
                byRow.put(decision.getRowId(), decision);
            }
            DataFormatter formatter = new DataFormatter(Locale.ROOT);
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null) {
                    continue;
                }
                String rowId = formatter.formatCellValue(
                        row.getCell(rowIdColumn)).trim();
                AutoAnnotationDecision decision = byRow.get(rowId);
                if (decision == null) {
                    continue;
                }
                set(row, labelColumn, String.valueOf(decision.getRowLabel()));
                set(row, errorColumn, join(decision.getErrorColumns()));
                set(row, commentColumn, comment(decision));
            }
            createDetailSheet(workbook, decisions);
            workbook.write(buffer);
            bytes = buffer.toByteArray();
        } catch (IOException exception) {
            LOGGER.error("生成自动标注 Excel 失败，sourceFile={}，targetFile={}",
                    source.getFileName(), target.getFileName(), exception);
            throw new IllegalStateException("生成自动标注 Excel 失败", exception);
        }
        try {
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            try (OutputStream outputStream = Files.newOutputStream(target,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                outputStream.write(bytes);
            }
            LOGGER.info("自动标注 Excel 回写完成，targetFile={}，decisionCount={}",
                    target.getFileName(), decisions.size());
            return target;
        } catch (IOException exception) {
            LOGGER.error("写入自动标注 Excel 失败，targetFile={}",
                    target.getFileName(), exception);
            throw new IllegalStateException("写入自动标注 Excel 失败", exception);
        }
    }

    private static void createDetailSheet(Workbook workbook,
                                          List<AutoAnnotationDecision> decisions) {
        int existing = workbook.getSheetIndex(DETAIL_SHEET);
        if (existing >= 0) {
            workbook.removeSheetAt(existing);
        }
        Sheet detail = workbook.createSheet(DETAIL_SHEET);
        write(detail.createRow(0), Arrays.asList("_row_id", "_row_label",
                "_error_columns", "confidence", "requires_review", "reason"));
        int rowIndex = 1;
        for (AutoAnnotationDecision decision : decisions) {
            write(detail.createRow(rowIndex++), Arrays.asList(
                    decision.getRowId(), String.valueOf(decision.getRowLabel()),
                    join(decision.getErrorColumns()),
                    String.format(Locale.ROOT, "%.4f", decision.getConfidence()),
                    String.valueOf(decision.isRequiresReview()),
                    limited(decision.getReason())));
        }
        detail.createFreezePane(0, 1);
        detail.setColumnWidth(0, 36 * 256);
        detail.setColumnWidth(1, 16 * 256);
        detail.setColumnWidth(2, 36 * 256);
        detail.setColumnWidth(3, 16 * 256);
        detail.setColumnWidth(4, 20 * 256);
        detail.setColumnWidth(5, 80 * 256);
    }

    private static String comment(AutoAnnotationDecision decision) {
        StringBuilder value = new StringBuilder("自动标注：置信度 ")
                .append(String.format(Locale.ROOT, "%.4f",
                        decision.getConfidence()))
                .append("；原因：").append(decision.getReason());
        if (decision.isRequiresReview()) {
            value.append("；请人工复核");
        }
        return limited(value.toString());
    }

    private static Map<String, Integer> indexes(Row header) {
        Map<String, Integer> result = new LinkedHashMap<String, Integer>();
        DataFormatter formatter = new DataFormatter(Locale.ROOT);
        for (Cell cell : header) {
            result.put(formatter.formatCellValue(cell).trim(),
                    Integer.valueOf(cell.getColumnIndex()));
        }
        return result;
    }

    private static int required(Map<String, Integer> indexes, String name) {
        Integer value = indexes.get(name);
        if (value == null) {
            throw new IllegalArgumentException("标注数据缺少字段：" + name);
        }
        return value.intValue();
    }

    private static void set(Row row, int column, String value) {
        Cell cell = row.getCell(column, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
        cell.setCellValue(limited(value));
    }

    private static void write(Row row, List<String> values) {
        for (int index = 0; index < values.size(); index++) {
            row.createCell(index).setCellValue(limited(values.get(index)));
        }
    }

    private static String join(List<String> values) {
        StringBuilder result = new StringBuilder();
        for (String value : values) {
            if (result.length() > 0) {
                result.append(',');
            }
            result.append(value);
        }
        return result.toString();
    }

    private static String limited(String value) {
        String text = value == null ? "" : value;
        return text.length() <= MAX_CELL_CHARS ? text
                : text.substring(0, MAX_CELL_CHARS);
    }
}
