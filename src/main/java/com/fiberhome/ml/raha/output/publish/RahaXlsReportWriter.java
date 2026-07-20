package com.fiberhome.ml.raha.output.publish;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

/**
 * 写入训练报告和检测明细使用的简单 HSSF 工作簿。
 */
public final class RahaXlsReportWriter {

    public Path write(Path outputPath,
                      String sheetName,
                      List<String> headers,
                      List<Map<String, Object>> rows) {
        if (outputPath == null || headers == null || headers.isEmpty()
                || rows == null) {
            throw new IllegalArgumentException("Excel 报告输出参数不能为空");
        }
        Path target = outputPath.toAbsolutePath().normalize();
        Workbook workbook = new HSSFWorkbook();
        try {
            Sheet sheet = workbook.createSheet(safeSheetName(sheetName));
            CellStyle headerStyle = headerStyle(workbook);
            writeHeader(sheet.createRow(0), headers, headerStyle);
            int rowIndex = 1;
            for (Map<String, Object> values : rows) {
                writeDataRow(sheet.createRow(rowIndex++), headers, values);
            }
            for (int index = 0; index < headers.size(); index++) {
                sheet.setColumnWidth(index, 24 * 256);
            }
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            try (OutputStream output = Files.newOutputStream(target,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                workbook.write(output);
            }
            return target;
        } catch (IOException exception) {
            throw new IllegalStateException("写入 Excel 报告失败", exception);
        } finally {
            try {
                workbook.close();
            } catch (IOException exception) {
                // 关闭失败不影响主流程结果，写入阶段已经完成。
            }
        }
    }

    private static void writeHeader(Row row,
                                    List<String> headers,
                                    CellStyle style) {
        for (int index = 0; index < headers.size(); index++) {
            Cell cell = row.createCell(index);
            cell.setCellValue(headers.get(index));
            cell.setCellStyle(style);
        }
    }

    private static void writeDataRow(Row row,
                                     List<String> headers,
                                     Map<String, Object> values) {
        for (int index = 0; index < headers.size(); index++) {
            Object value = values == null ? null : values.get(headers.get(index));
            row.createCell(index).setCellValue(value == null
                    ? "" : String.valueOf(value));
        }
    }

    private static CellStyle headerStyle(Workbook workbook) {
        CellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        Font font = workbook.createFont();
        font.setBold(true);
        style.setFont(font);
        return style;
    }

    private static String safeSheetName(String sheetName) {
        String value = sheetName == null || sheetName.trim().isEmpty()
                ? "report" : sheetName.trim();
        value = value.replaceAll("[\\\\/?*\\[\\]:]", "_");
        return value.length() > 31 ? value.substring(0, 31) : value;
    }
}
