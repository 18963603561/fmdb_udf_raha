package com.fiberhome.ml.raha.app;

import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT;
import com.fiberhome.ml.raha.udf.F_DW_DETRUN;
import com.fiberhome.ml.raha.udf.F_DW_DETTRAIN;
import com.fiberhome.ml.raha.udf.RahaUdfField;
import com.fiberhome.ml.raha.udf.RahaUdfFields;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * 在 Spark Driver 侧直接执行 Raha 三个 GenericUDF 的本地调试入口。
 *
 * <p>该入口绕过 spark-sql 命令，直接实例化 UDF 并调用
 * {@link GenericUDF#evaluate(GenericUDF.DeferredObject[])}，适合在 IDE 中单步调试
 * 采样、训练和检测函数。</p>
 */
public final class RahaUdfDriverApp {

    /** 命令行函数名参数下标。 */
    private static final int FUNCTION_ARG_INDEX = 0;
    /** 命令行请求参数下标。 */
    private static final int REQUEST_ARG_INDEX = 1;
    /** 命令行输出路径参数下标。 */
    private static final int OUTPUT_ARG_INDEX = 2;
    /** 默认调试函数配置项，用于 main 无参数时选择 COLLECT 、 TRAIN 或 DETECT 。 */
    private static final String DEBUG_FUNCTION_PROPERTY ="COLLECT";
    /** 默认调试函数值，未配置系统属性时使用该函数。 */
    private static final String DEFAULT_DEBUG_FUNCTION = "COLLECT";
    /** 默认调试数据根目录配置项，用于覆盖 datasets 下的默认 person_info 目录。 */
    private static final String DEBUG_DATASET_ROOT_PROPERTY =
            "raha.udf.debug.dataset-root";
    /** 默认调试数据根目录，统一维护 person_info 的 SQL、请求、标注和输出。 */
    private static final String DEFAULT_DATASET_ROOT =
            "datasets/person_info";
    /** 本地 Hadoop 根目录配置项，Windows 调试时用于定位 winutils.exe 和 hadoop.dll。 */
    private static final String HADOOP_HOME_PROPERTY = "hadoop.home.dir";
    /** 本机默认 Hadoop 根目录。 */
    private static final String DEFAULT_LOCAL_HADOOP_HOME =
            "C:/hadoop/hadoop-3.2.2";
    /** Windows Hadoop Native 动态库文件名。 */
    private static final String HADOOP_DLL_NAME = "hadoop.dll";
    /** Windows Hadoop 辅助程序文件名。 */
    private static final String WINUTILS_NAME = "winutils.exe";

    private RahaUdfDriverApp() {
    }

    /**
     * 命令行入口，用于在 Spark Driver 进程内直接调用 Raha 三个 GenericUDF。
     *
     * <p>参数格式：</p>
     * <pre>
     * RahaUdfDriverApp &lt;functionName&gt; &lt;request|@requestFile&gt; [outputJsonPath]
     * </pre>
     *
     * <p>参数说明：</p>
     * <pre>
     * functionName    支持 COLLECT、TRAIN、DETECT，也支持完整函数名 F_DW_DETCOLLECT、F_DW_DETTRAIN、F_DW_DETRUN。
     * request         可以直接传 JSON 字符串；也可以使用 @ 开头传请求文件路径。
     * outputJsonPath  可选，传入后会把 UDF 二维表返回值转换为 JSON 并写入该文件。
     * </pre>
     *
     * <p>无参数运行时会进入默认调试模式，默认读取
     * {@code datasets/person_info/requests} 下的请求文件，结果写入
     * {@code datasets/person_info/out}。默认函数为 COLLECT，可通过 VM 参数切换：</p>
     * <pre>
     * -Draha.udf.debug.function=TRAIN
     * -Draha.udf.debug.function=DETECT
     * -Draha.udf.debug.dataset-root=datasets/person_info
     * </pre>
     *
     * <p>Windows 本地 JDK 17 调试建议保留以下 VM 参数：</p>
     * <pre>
     * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED
     * </pre>
     *
     * <p>本地 Maven 调用示例：</p>
     * <pre>
     * mvn -q "-DskipTests" exec:java "-Dexec.mainClass=com.fiberhome.ml.raha.app.RahaUdfDriverApp" "-Dexec.classpathScope=test" "-Dexec.args=COLLECT @datasets/person_info/requests/collect-local.json datasets/person_info/out/collect-local-result.json"
     * mvn -q "-DskipTests" exec:java "-Dexec.mainClass=com.fiberhome.ml.raha.app.RahaUdfDriverApp" "-Dexec.classpathScope=test" "-Dexec.args=TRAIN @datasets/person_info/requests/train-local.json datasets/person_info/out/train-local-result.json"
     * mvn -q "-DskipTests" exec:java "-Dexec.mainClass=com.fiberhome.ml.raha.app.RahaUdfDriverApp" "-Dexec.classpathScope=test" "-Dexec.args=DETECT @datasets/person_info/requests/detect-local-limit450.json datasets/person_info/out/detect-local-limit450-result.json"
     * </pre>
     *
     * @param args 命令行参数；为空时使用默认调试参数
     * @throws Exception UDF 初始化、执行或结果写出失败时抛出
     */
    public static void main(String[] args) throws Exception {
        prepareLocalWindowsHadoopNative();
        String[] resolvedArgs = args == null || args.length == 0
                ? defaultDebugArgs() : args;
        if (args == null || args.length == 0) {
            System.out.println("RAHA_UDF_DEFAULT_ARGS="
                    + Arrays.toString(resolvedArgs));
        }
        if (resolvedArgs.length < 2) {
            throw new IllegalArgumentException(
                    "用法：RahaUdfDriverApp <functionName> <request|@requestFile> [outputJsonPath]");
        }
        String functionName = resolvedArgs[FUNCTION_ARG_INDEX];
        String request = readRequest(resolvedArgs[REQUEST_ARG_INDEX]);
        SparkSession spark = SparkSession.builder()
                .appName("RahaUdfDriverApp-" + functionName)
                .master(System.getProperty("raha.spark.master", "local[*]"))
                .enableHiveSupport()
                .getOrCreate();
        SparkSession.setActiveSession(spark);
        SparkSession.setDefaultSession(spark);
        try {
            spark.sparkContext().setLogLevel("WARN");
            List<Map<String, Object>> rows = execute(functionName, request);
            String json = FmdbJsonCodec.write(rows);
            if (resolvedArgs.length > OUTPUT_ARG_INDEX
                    && resolvedArgs[OUTPUT_ARG_INDEX] != null
                    && !resolvedArgs[OUTPUT_ARG_INDEX].trim().isEmpty()) {
                Path outputPath = Paths.get(resolvedArgs[OUTPUT_ARG_INDEX]);
                if (outputPath.getParent() != null) {
                    Files.createDirectories(outputPath.getParent());
                }
                Files.write(outputPath, json.getBytes(StandardCharsets.UTF_8));
                System.out.println("RAHA_UDF_RESULT_JSON="
                        + outputPath.toAbsolutePath().normalize());
            }
            System.out.println(json);
            printStageTimings(spark, rows);
        } finally {
            spark.stop();
        }
    }

    /**
     * 使用默认采样请求执行本地调试。
     *
     * <p>适用于 IDE 中直接调用该方法调试 {@code F_DW_DETCOLLECT}。</p>
     *
     * @throws Exception UDF 执行失败时抛出
     */
    public static void runDefaultCollect() throws Exception {
        main(defaultCollectArgs());
    }

    /**
     * 使用默认训练请求执行本地调试。
     *
     * <p>训练依赖已经存在的采样批次和已标注 Excel，请先完成采样和自动标注。</p>
     *
     * @throws Exception UDF 执行失败时抛出
     */
    public static void runDefaultTrain() throws Exception {
        main(defaultTrainArgs());
    }

    /**
     * 使用默认检测请求执行本地调试。
     *
     * <p>检测默认使用 {@code select * from dw.person_info limit 450}，并依赖已发布模型集合。</p>
     *
     * @throws Exception UDF 执行失败时抛出
     */
    public static void runDefaultDetect() throws Exception {
        main(defaultDetectArgs());
    }

    /**
     * 构造默认采样参数。
     *
     * @return 可直接传给 {@link #main(String[])} 的采样参数
     */
    public static String[] defaultCollectArgs() {
        Path datasetRoot = defaultDatasetRoot();
        return new String[] {
                "COLLECT",
                "@" + datasetRoot.resolve("requests/collect-local.json"),
                datasetRoot.resolve("out/collect-local-result.json").toString()
        };
    }

    /**
     * 构造默认训练参数。
     *
     * @return 可直接传给 {@link #main(String[])} 的训练参数
     */
    public static String[] defaultTrainArgs() {
        Path datasetRoot = defaultDatasetRoot();
        return new String[] {
                "TRAIN",
                "@" + datasetRoot.resolve("requests/train-local.json"),
                datasetRoot.resolve("out/train-local-result.json").toString()
        };
    }

    /**
     * 构造默认检测参数。
     *
     * @return 可直接传给 {@link #main(String[])} 的检测参数
     */
    public static String[] defaultDetectArgs() {
        Path datasetRoot = defaultDatasetRoot();
        return new String[] {
                "DETECT",
                "@" + datasetRoot.resolve("requests/detect-local-limit450.json"),
                datasetRoot.resolve("out/detect-local-limit450-result.json")
                        .toString()
        };
    }

    /**
     * 准备 Windows 本地 Hadoop Native 环境。
     *
     * <p>IDE 直接运行 main 时通常不会携带 {@code -Dhadoop.home.dir} 和
     * {@code -Djava.library.path}。该方法在 Spark 初始化前设置默认 Hadoop 目录，并显式加载
     * {@code hadoop.dll}，避免本地调试因 {@code NativeIO$Windows.access0} 缺失失败。</p>
     */
    public static void prepareLocalWindowsHadoopNative() {
        if (!isWindows()) {
            return;
        }
        String hadoopHome = trimToNull(System.getProperty(HADOOP_HOME_PROPERTY));
        if (hadoopHome == null) {
            hadoopHome = trimToNull(System.getenv("HADOOP_HOME"));
        }
        if (hadoopHome == null) {
            hadoopHome = DEFAULT_LOCAL_HADOOP_HOME;
        }
        Path hadoopHomePath = Paths.get(hadoopHome).toAbsolutePath().normalize();
        Path hadoopBinPath = hadoopHomePath.resolve("bin");
        Path hadoopDllPath = hadoopBinPath.resolve(HADOOP_DLL_NAME);
        Path winutilsPath = hadoopBinPath.resolve(WINUTILS_NAME);
        if (!Files.isRegularFile(hadoopDllPath)
                || !Files.isRegularFile(winutilsPath)) {
            throw new IllegalStateException(
                    "Windows 本地 Hadoop 依赖不存在，请确认目录包含 hadoop.dll 和 winutils.exe："
                            + hadoopBinPath);
        }
        System.setProperty(HADOOP_HOME_PROPERTY, hadoopHomePath.toString());
        appendJavaLibraryPath(hadoopBinPath);
        try {
            System.load(hadoopDllPath.toString());
            System.out.println("RAHA_HADOOP_NATIVE_LOADED=" + hadoopDllPath);
        } catch (UnsatisfiedLinkError error) {
            String message = error.getMessage();
            if (message != null && message.contains("already loaded")) {
                return;
            }
            throw new IllegalStateException(
                    "加载 Windows Hadoop Native 库失败，请检查 hadoop.dll 是否与 JVM 位数匹配："
                            + hadoopDllPath,
                    error);
        }
    }

    private static String[] defaultDebugArgs() {
        String functionName = System.getProperty(DEBUG_FUNCTION_PROPERTY,
                DEFAULT_DEBUG_FUNCTION);
        String normalized = normalize(functionName);
        System.out.println("============RAHA_UDF_DEBUG_FUNCTION==============" + normalized);
        // 空参数调试时根据系统属性选择默认函数，避免每次修改命令行参数。
        if ("F_DW_DETCOLLECT".equals(normalized) || "COLLECT".equals(normalized)) {
            return defaultCollectArgs();
        }
        if ("F_DW_DETTRAIN".equals(normalized) || "TRAIN".equals(normalized)) {
            return defaultTrainArgs();
        }
        if ("F_DW_DETRUN".equals(normalized) || "DETECT".equals(normalized)
                || "RUN".equals(normalized)) {
            return defaultDetectArgs();
        }
        throw new IllegalArgumentException("不支持的默认调试函数："
                + functionName);
    }

    private static Path defaultDatasetRoot() {
        String configured = System.getProperty(DEBUG_DATASET_ROOT_PROPERTY,
                DEFAULT_DATASET_ROOT);
        return Paths.get(configured);
    }

    private static List<Map<String, Object>> execute(String functionName,
                                                     String request)
            throws Exception {
        GenericUDF udf = createUdf(functionName);
        ObjectInspector stringInspector =
                PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        udf.initialize(new ObjectInspector[] {stringInspector});
        Object evaluated = udf.evaluate(new GenericUDF.DeferredObject[] {
                new GenericUDF.DeferredJavaObject(request)
        });
        return rows(functionName, evaluated);
    }

    private static GenericUDF createUdf(String functionName) {
        String normalized = normalize(functionName);
        if ("F_DW_DETCOLLECT".equals(normalized) || "COLLECT".equals(normalized)) {
            return new F_DW_DETCOLLECT();
        }
        if ("F_DW_DETTRAIN".equals(normalized) || "TRAIN".equals(normalized)) {
            return new F_DW_DETTRAIN();
        }
        if ("F_DW_DETRUN".equals(normalized) || "DETECT".equals(normalized)
                || "RUN".equals(normalized)) {
            return new F_DW_DETRUN();
        }
        throw new IllegalArgumentException("不支持的 Raha UDF 函数："
                + functionName);
    }

    private static List<RahaUdfField> fields(String functionName) {
        String normalized = normalize(functionName);
        if ("F_DW_DETCOLLECT".equals(normalized) || "COLLECT".equals(normalized)) {
            return RahaUdfFields.COLLECT;
        }
        if ("F_DW_DETTRAIN".equals(normalized) || "TRAIN".equals(normalized)) {
            return RahaUdfFields.TRAIN;
        }
        if ("F_DW_DETRUN".equals(normalized) || "DETECT".equals(normalized)
                || "RUN".equals(normalized)) {
            return RahaUdfFields.DETECT;
        }
        throw new IllegalArgumentException("不支持的 Raha UDF 函数："
                + functionName);
    }

    private static List<Map<String, Object>> rows(String functionName,
                                                  Object evaluated) {
        if (!(evaluated instanceof List)) {
            throw new IllegalStateException("Raha UDF 返回值不是二维结果列表");
        }
        List<RahaUdfField> fields = fields(functionName);
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        for (Object item : (List<?>) evaluated) {
            if (!(item instanceof Object[])) {
                throw new IllegalStateException("Raha UDF 返回行不是结构数组");
            }
            Object[] values = (Object[]) item;
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            for (int index = 0; index < fields.size(); index++) {
                Object value = index < values.length ? values[index] : null;
                row.put(fields.get(index).getName(), value);
            }
            result.add(row);
        }
        return result;
    }

    /**
     * 从任务阶段表读取并打印本次函数执行的阶段耗时。
     *
     * <p>该输出只用于本地驱动调试，不改变 UDF 原始二维表结果。</p>
     */
    private static void printStageTimings(SparkSession spark,
                                          List<Map<String, Object>> rows) {
        String jobId = firstJobId(rows);
        if (jobId == null) {
            return;
        }
        Map<String, Object> summary = new LinkedHashMap<String, Object>();
        summary.put("jobId", jobId);
        try {
            if (!stageTableExists(spark)) {
                summary.put("stageTimingAvailable", Boolean.FALSE);
                summary.put("reason", "stage table not found");
                System.out.println("RAHA_STAGE_TIMINGS_JSON="
                        + FmdbJsonCodec.write(summary));
                return;
            }
            List<Row> stageRows = spark.table("dw.raha_job_stage_attempt")
                    .filter(functions.col("job_id").equalTo(jobId))
                    .select("stage_id", "stage_type", "attempt_id", "status",
                            "error_code", "started_at", "completed_at")
                    .orderBy(functions.col("started_at").asc(),
                            functions.col("attempt_id").asc())
                    .collectAsList();
            List<Map<String, Object>> stages =
                    new ArrayList<Map<String, Object>>(stageRows.size());
            long totalMillis = 0L;
            for (Row row : stageRows) {
                long startedAt = longValue(row.getAs("started_at"));
                long completedAt = longValue(row.getAs("completed_at"));
                long elapsedMillis = Math.max(0L, completedAt - startedAt);
                totalMillis += elapsedMillis;
                Map<String, Object> stage = new LinkedHashMap<String, Object>();
                stage.put("stageId", row.getAs("stage_id"));
                stage.put("stageType", row.getAs("stage_type"));
                stage.put("attemptId", row.getAs("attempt_id"));
                stage.put("status", row.getAs("status"));
                stage.put("errorCode", row.getAs("error_code"));
                stage.put("startedAt", Long.valueOf(startedAt));
                stage.put("completedAt", Long.valueOf(completedAt));
                stage.put("elapsedMillis", Long.valueOf(elapsedMillis));
                stages.add(stage);
            }
            summary.put("stageTimingAvailable", Boolean.TRUE);
            summary.put("stageCount", Integer.valueOf(stages.size()));
            summary.put("totalStageMillis", Long.valueOf(totalMillis));
            summary.put("stages", stages);
        } catch (RuntimeException exception) {
            summary.put("stageTimingAvailable", Boolean.FALSE);
            summary.put("reason", exception.getMessage());
        }
        System.out.println("RAHA_STAGE_TIMINGS_JSON="
                + FmdbJsonCodec.write(summary));
    }

    private static boolean stageTableExists(SparkSession spark) {
        try {
            return spark.catalog().tableExists("dw.raha_job_stage_attempt");
        } catch (RuntimeException exception) {
            return false;
        }
    }

    private static String firstJobId(List<Map<String, Object>> rows) {
        if (rows == null) {
            return null;
        }
        for (Map<String, Object> row : rows) {
            Object value = row.get("jobId");
            if (value != null && !String.valueOf(value).trim().isEmpty()) {
                return String.valueOf(value).trim();
            }
        }
        return null;
    }

    private static long longValue(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private static String readRequest(String argument) throws IOException {
        if (argument != null && argument.startsWith("@")) {
            byte[] bytes = Files.readAllBytes(Paths.get(argument.substring(1)));
            return new String(bytes, StandardCharsets.UTF_8).trim();
        }
        return argument;
    }

    private static void appendJavaLibraryPath(Path expectedPath) {
        String libraryPath = System.getProperty("java.library.path", "");
        if (!containsPath(libraryPath, expectedPath)) {
            System.setProperty("java.library.path", expectedPath
                    + File.pathSeparator + libraryPath);
        }
    }

    private static boolean isWindows() {
        String osName = System.getProperty("os.name", "");
        return osName.toLowerCase(Locale.ROOT).contains("win");
    }

    private static boolean containsPath(String libraryPath, Path expectedPath) {
        if (libraryPath == null || libraryPath.trim().isEmpty()) {
            return false;
        }
        String expected = expectedPath.toAbsolutePath().normalize().toString();
        String[] parts = libraryPath.split(java.util.regex.Pattern.quote(
                File.pathSeparator), -1);
        for (String part : parts) {
            if (part == null || part.trim().isEmpty()) {
                continue;
            }
            if (expected.equalsIgnoreCase(Paths.get(part).toAbsolutePath()
                    .normalize().toString())) {
                return true;
            }
        }
        return false;
    }

    private static String trimToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private static String normalize(String functionName) {
        return functionName == null ? ""
                : functionName.trim().toUpperCase(Locale.ROOT);
    }
}
