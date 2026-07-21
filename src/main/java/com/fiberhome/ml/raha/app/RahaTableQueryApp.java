package com.fiberhome.ml.raha.app;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按表名查询 Spark Hive 表并把结果打印到控制台、导出为 xls 的本地调试入口。
 *
 * <p>默认查询 {@code dw.person_info1} 前 100 行，并把结果写入
 * {@code datasets/person_info/out}。可通过命令行参数覆盖表名、行数、列、过滤条件和 xls 输出文件。</p>
 */
public final class RahaTableQueryApp {

    /** 日志记录器。 */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RahaTableQueryApp.class);
    /** 默认查询表配置项。 */
    private static final String DEFAULT_TABLE_PROPERTY =
            "raha.query.default-table";
    /** 默认查询行数配置项。 */
    private static final String DEFAULT_LIMIT_PROPERTY =
            "raha.query.default-limit";
    /** 默认 xls 输出路径配置项。 */
    private static final String DEFAULT_XLS_PROPERTY =
            "raha.query.default-xls";
    /** 默认查询表，配合 person_info 本地数据集使用。 */
    private static final String DEFAULT_TABLE = "dw.raha_job_run";
    /** 默认查询行数，避免一次性拉取过多数据到 Driver。 */
    private static final int DEFAULT_LIMIT = 100;
    /** 控制台最多打印行数，和默认查询行数保持一致。 */
    private static final int DEFAULT_SHOW_ROWS = 100;
    /** xls 单表最大行数，超过该值会主动拒绝，保护本地调试进程内存。 */
    private static final int MAX_XLS_ROWS = 10000;
    /** 合法表名或列名片段，避免把任意 SQL 拼进表名位置。 */
    private static final Pattern IDENTIFIER_PART =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private RahaTableQueryApp() {
    }

    /**
     * 命令行入口。
     *
     * <p>推荐参数：</p>
     * <pre>
     * RahaTableQueryApp --table dw.person_info1 --limit 100 --xls datasets/person_info/out/person_info1.xls
     * RahaTableQueryApp --table dw.person_info1 --columns name,age,email --where "age &gt; 60"
     * </pre>
     *
     * <p>兼容位置参数：</p>
     * <pre>
     * RahaTableQueryApp dw.person_info1 100 datasets/person_info/out/person_info1.xls
     * </pre>
     *
     * @param args 命令行参数；为空时使用默认表、默认行数和默认 xls 路径
     * @throws Exception 查询或导出失败时抛出
     */
    public static void main(String[] args) throws Exception {
        QueryOptions options = QueryOptions.parse(args);
        RahaUdfDriverApp.prepareLocalWindowsHadoopNative();
        SparkSession spark = SparkSession.builder()
                .appName("RahaTableQueryApp-" + options.tableName)
                .master(System.getProperty("raha.spark.master", "local[*]"))
                .enableHiveSupport()
                .getOrCreate();
        SparkSession.setActiveSession(spark);
        SparkSession.setDefaultSession(spark);
        try {
            spark.sparkContext().setLogLevel("WARN");
            Dataset<org.apache.spark.sql.Row> result = query(spark, options);
            printToConsole(result, options);
            Path xlsPath = writeXls(result, options.xlsPath);
            System.out.println("RAHA_TABLE_QUERY_XLS="
                    + xlsPath.toAbsolutePath().normalize());
        } catch (Exception exception) {
            LOGGER.error("查询表失败，tableName={}，limit={}，xlsPath={}",
                    options.tableName, Integer.valueOf(options.limit),
                    options.xlsPath, exception);
            throw exception;
        } finally {
            spark.stop();
        }
    }

    private static Dataset<org.apache.spark.sql.Row> query(SparkSession spark,
                                                           QueryOptions options) {
        LOGGER.info("开始查询表，tableName={}，limit={}，columns={}，where={}",
                options.tableName, Integer.valueOf(options.limit),
                options.columns, options.whereClause);
        Dataset<org.apache.spark.sql.Row> dataset = spark.table(options.tableName);
        if (options.whereClause != null) {
            // 过滤条件来自本地调试参数，保留 Spark SQL 表达式能力。
            dataset = dataset.where(options.whereClause);
        }
        if (!options.columns.isEmpty()) {
            dataset = dataset.select(toColumnArray(options.columns));
        }
        Dataset<org.apache.spark.sql.Row> result = dataset.limit(options.limit);
        LOGGER.info("表查询计划构造完成，tableName={}，limit={}",
                options.tableName, Integer.valueOf(options.limit));
        return result;
    }

    private static void printToConsole(Dataset<org.apache.spark.sql.Row> result,
                                       QueryOptions options) {
        LOGGER.info("开始打印查询结果，tableName={}，showRows={}",
                options.tableName, Integer.valueOf(DEFAULT_SHOW_ROWS));
        result.show(DEFAULT_SHOW_ROWS, false);
        LOGGER.info("查询结果打印完成，tableName={}", options.tableName);
    }

    private static Path writeXls(Dataset<org.apache.spark.sql.Row> result,
                                 Path outputPath) {
        LOGGER.info("开始导出 xls，outputPath={}", outputPath);
        List<org.apache.spark.sql.Row> rows = result.collectAsList();
        if (rows.size() > MAX_XLS_ROWS) {
            throw new IllegalArgumentException(
                    "xls 导出行数超过上限：" + rows.size());
        }
        Path target = outputPath.toAbsolutePath().normalize();
        Workbook workbook = new HSSFWorkbook();
        try {
            Sheet sheet = workbook.createSheet("query");
            StructType schema = result.schema();
            CellStyle headerStyle = headerStyle(workbook);
            writeHeader(sheet.createRow(0), schema.fields(), headerStyle);
            int[] widths = initialWidths(schema);
            for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
                writeDataRow(sheet.createRow(rowIndex + 1), rows.get(rowIndex),
                        schema, widths);
            }
            applyColumnWidths(sheet, widths);
            if (target.getParent() != null) {
                // 写入本地文件系统前创建父目录，方便空目录环境直接运行。
                Files.createDirectories(target.getParent());
            }
            try (OutputStream output = Files.newOutputStream(target,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE)) {
                workbook.write(output);
            }
            LOGGER.info("xls 导出完成，outputPath={}，rowCount={}",
                    target, Integer.valueOf(rows.size()));
            return target;
        } catch (IOException exception) {
            LOGGER.error("写入 xls 失败，outputPath={}", target, exception);
            throw new IllegalStateException("写入 xls 失败：" + target, exception);
        } finally {
            try {
                workbook.close();
            } catch (IOException exception) {
                LOGGER.warn("关闭 xls 工作簿失败，outputPath={}", target,
                        exception);
            }
        }
    }

    private static void writeHeader(Row row,
                                    StructField[] fields,
                                    CellStyle style) {
        for (int index = 0; index < fields.length; index++) {
            Cell cell = row.createCell(index);
            cell.setCellValue(fields[index].name());
            cell.setCellStyle(style);
        }
    }

    private static void writeDataRow(Row excelRow,
                                     org.apache.spark.sql.Row sparkRow,
                                     StructType schema,
                                     int[] widths) {
        for (int index = 0; index < schema.fields().length; index++) {
            Object value = sparkRow.isNullAt(index) ? null : sparkRow.get(index);
            Cell cell = excelRow.createCell(index);
            setCellValue(cell, value);
            widths[index] = Math.max(widths[index], displayLength(value));
        }
    }

    private static void setCellValue(Cell cell,
                                     Object value) {
        if (value == null) {
            cell.setCellValue("");
            return;
        }
        if (value instanceof Integer || value instanceof Long
                || value instanceof Short || value instanceof Byte
                || value instanceof Float || value instanceof Double
                || value instanceof BigDecimal) {
            cell.setCellValue(Double.parseDouble(String.valueOf(value)));
            return;
        }
        if (value instanceof Boolean) {
            cell.setCellValue(((Boolean) value).booleanValue());
            return;
        }
        if (value instanceof Date || value instanceof Timestamp) {
            cell.setCellValue(String.valueOf(value));
            return;
        }
        cell.setCellValue(String.valueOf(value));
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

    private static int[] initialWidths(StructType schema) {
        StructField[] fields = schema.fields();
        int[] widths = new int[fields.length];
        for (int index = 0; index < fields.length; index++) {
            widths[index] = displayLength(fields[index].name());
        }
        return widths;
    }

    private static void applyColumnWidths(Sheet sheet,
                                          int[] widths) {
        for (int index = 0; index < widths.length; index++) {
            int width = Math.max(10, Math.min(widths[index] + 2, 60));
            sheet.setColumnWidth(index, width * 256);
        }
    }

    private static int displayLength(Object value) {
        return value == null ? 0 : String.valueOf(value).length();
    }

    private static org.apache.spark.sql.Column[] toColumnArray(
            List<String> columns) {
        org.apache.spark.sql.Column[] result =
                new org.apache.spark.sql.Column[columns.size()];
        for (int index = 0; index < columns.size(); index++) {
            result[index] = org.apache.spark.sql.functions.col(
                    columns.get(index));
        }
        return result;
    }

    private static String requireIdentifier(String value,
                                            String description) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            throw new IllegalArgumentException(description + "不能为空");
        }
        String[] parts = trimmed.split("\\.");
        for (String part : parts) {
            if (!IDENTIFIER_PART.matcher(part).matches()) {
                throw new IllegalArgumentException(
                        description + "只能包含普通标识符：" + value);
            }
        }
        return trimmed;
    }

    private static List<String> parseColumns(String value) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            return new ArrayList<String>();
        }
        List<String> result = new ArrayList<String>();
        for (String column : trimmed.split(",")) {
            result.add(requireIdentifier(column.trim(), "查询列"));
        }
        return result;
    }

    private static int parseLimit(String value) {
        int parsed = Integer.parseInt(value);
        if (parsed <= 0 || parsed > MAX_XLS_ROWS) {
            throw new IllegalArgumentException(
                    "limit 必须在 1 到 " + MAX_XLS_ROWS + " 之间");
        }
        return parsed;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Path defaultXlsPath(String tableName) {
        String configured = trimToNull(System.getProperty(DEFAULT_XLS_PROPERTY));
        if (configured != null) {
            return Paths.get(configured);
        }
        String safeName = tableName.replace('.', '_');
        return Paths.get("datasets/person_info/out")
                .resolve(safeName + "-query.xls");
    }

    private static final class QueryOptions {

        /** 查询目标表名，支持库名加表名。 */
        private final String tableName;
        /** 查询行数上限，也是 xls 导出行数上限。 */
        private final int limit;
        /** 需要查询的列名；为空时查询全部列。 */
        private final List<String> columns;
        /** 可选过滤条件，使用 Spark SQL 表达式。 */
        private final String whereClause;
        /** xls 输出路径。 */
        private final Path xlsPath;

        private QueryOptions(String tableName,
                             int limit,
                             List<String> columns,
                             String whereClause,
                             Path xlsPath) {
            this.tableName = tableName;
            this.limit = limit;
            this.columns = columns;
            this.whereClause = whereClause;
            this.xlsPath = xlsPath;
        }

        private static QueryOptions parse(String[] args) {
            if (args == null || args.length == 0) {
                String table = requireIdentifier(System.getProperty(
                        DEFAULT_TABLE_PROPERTY, DEFAULT_TABLE), "查询表名");
                int limit = parseLimit(System.getProperty(
                        DEFAULT_LIMIT_PROPERTY, String.valueOf(DEFAULT_LIMIT)));
                return new QueryOptions(table, limit, new ArrayList<String>(),
                        null, defaultXlsPath(table));
            }
            if (args[0].startsWith("--")) {
                return parseNamed(args);
            }
            return parsePositional(args);
        }

        private static QueryOptions parseNamed(String[] args) {
            String table = null;
            int limit = DEFAULT_LIMIT;
            List<String> columns = new ArrayList<String>();
            String where = null;
            Path xls = null;
            for (int index = 0; index < args.length; index++) {
                String name = args[index];
                String value = requireOptionValue(args, ++index, name);
                if ("--table".equals(name)) {
                    table = requireIdentifier(value, "查询表名");
                } else if ("--limit".equals(name)) {
                    limit = parseLimit(value);
                } else if ("--columns".equals(name)) {
                    columns = parseColumns(value);
                } else if ("--where".equals(name)) {
                    where = trimToNull(value);
                } else if ("--xls".equals(name)) {
                    xls = Paths.get(value);
                } else {
                    throw new IllegalArgumentException("未知参数：" + name);
                }
            }
            if (table == null) {
                table = requireIdentifier(System.getProperty(
                        DEFAULT_TABLE_PROPERTY, DEFAULT_TABLE), "查询表名");
            }
            return new QueryOptions(table, limit, columns, where,
                    xls == null ? defaultXlsPath(table) : xls);
        }

        private static QueryOptions parsePositional(String[] args) {
            String table = requireIdentifier(args[0], "查询表名");
            int limit = args.length > 1 ? parseLimit(args[1]) : DEFAULT_LIMIT;
            Path xls = args.length > 2 ? Paths.get(args[2])
                    : defaultXlsPath(table);
            String where = args.length > 3 ? trimToNull(args[3]) : null;
            return new QueryOptions(table, limit, new ArrayList<String>(),
                    where, xls);
        }

        private static String requireOptionValue(String[] args,
                                                 int index,
                                                 String name) {
            if (index >= args.length || args[index].startsWith("--")) {
                throw new IllegalArgumentException("参数缺少取值：" + name);
            }
            return args[index];
        }

        @Override
        public String toString() {
            return "QueryOptions{"
                    + "tableName='" + tableName + '\''
                    + ", limit=" + limit
                    + ", columns=" + Arrays.toString(columns.toArray())
                    + ", whereClause='" + whereClause + '\''
                    + ", xlsPath=" + xlsPath
                    + '}';
        }
    }
}
