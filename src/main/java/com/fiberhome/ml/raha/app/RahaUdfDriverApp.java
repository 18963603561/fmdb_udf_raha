package com.fiberhome.ml.raha.app;

import com.fiberhome.ml.raha.repository.adapter.fmdb.support.FmdbJsonCodec;
import com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT;
import com.fiberhome.ml.raha.udf.F_DW_DETRUN;
import com.fiberhome.ml.raha.udf.F_DW_DETTRAIN;
import com.fiberhome.ml.raha.udf.RahaUdfField;
import com.fiberhome.ml.raha.udf.RahaUdfFields;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.spark.sql.SparkSession;

/**
 * 在 Spark 驱动侧直接执行 Raha 三个 GenericUDF，适合函数内部需要再次发起 Spark 作业的场景。
 */
public final class RahaUdfDriverApp {

    /** 命令行函数名参数下标。 */
    private static final int FUNCTION_ARG_INDEX = 0;
    /** 命令行请求参数下标。 */
    private static final int REQUEST_ARG_INDEX = 1;
    /** 命令行输出路径参数下标。 */
    private static final int OUTPUT_ARG_INDEX = 2;
    /** 默认调试函数配置项，用于 main 无参数时选择 COLLECT、TRAIN 或 DETECT。 */
    private static final String DEBUG_FUNCTION_PROPERTY =
            "raha.udf.debug.function";
    /** 默认调试运行目录配置项，用于 main 无参数时覆盖请求和输出文件根目录。 */
    private static final String DEBUG_RUN_ROOT_PROPERTY =
            "raha.udf.debug.run-root";
    /** 默认调试运行目录，保存本地三函数请求文件和结果文件。 */
    private static final String DEFAULT_DEBUG_RUN_ROOT =
            "doc/20260721/notes/local-udf-run-202607211230";

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
     * <p>Windows 本地执行前建议先准备环境：</p>
     * <pre>
     * $env:HADOOP_HOME='C:\hadoop\hadoop-3.2.2'
     * $env:Path='C:\hadoop\hadoop-3.2.2\bin;' + $env:Path
     * $env:SPARK_LOCAL_IP='127.0.0.1'
     * $env:JAVA_TOOL_OPTIONS='--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED'
     * </pre>
     *
     * <p>使用 Maven 在本地调用采样函数：</p>
     * <pre>
     * mvn -q "-Denforcer.skip=true" "-DskipTests" exec:java "-Dexec.mainClass=com.fiberhome.ml.raha.app.RahaUdfDriverApp" "-Dexec.classpathScope=test" "-Dexec.args=COLLECT @doc\20260721\notes\local-udf-run-202607211230\requests\collect-local.json doc\20260721\notes\local-udf-run-202607211230\outputs\collect-local-result.json" "-Dexec.jvmArgs=-Djava.library.path=C:\hadoop\hadoop-3.2.2\bin -Dhadoop.home.dir=C:\hadoop\hadoop-3.2.2 -Dspark.master=local[*]"
     * </pre>
     *
     * <p>使用 Maven 在本地调用训练函数：</p>
     * <pre>
     * mvn -q "-Denforcer.skip=true" "-DskipTests" exec:java "-Dexec.mainClass=com.fiberhome.ml.raha.app.RahaUdfDriverApp" "-Dexec.classpathScope=test" "-Dexec.args=TRAIN @doc\20260721\notes\local-udf-run-202607211230\requests\train-local.json doc\20260721\notes\local-udf-run-202607211230\outputs\train-local-result.json" "-Dexec.jvmArgs=-Djava.library.path=C:\hadoop\hadoop-3.2.2\bin -Dhadoop.home.dir=C:\hadoop\hadoop-3.2.2 -Dspark.master=local[*]"
     * </pre>
     *
     * <p>使用 Maven 在本地调用检测函数，检测 SQL 使用 {@code select * from dw.person_info limit 450}：</p>
     * <pre>
     * mvn -q "-Denforcer.skip=true" "-DskipTests" exec:java "-Dexec.mainClass=com.fiberhome.ml.raha.app.RahaUdfDriverApp" "-Dexec.classpathScope=test" "-Dexec.args=DETECT @doc\20260721\notes\local-udf-run-202607211230\requests\detect-local-limit450.json doc\20260721\notes\local-udf-run-202607211230\outputs\detect-local-limit450-result.json" "-Dexec.jvmArgs=-Djava.library.path=C:\hadoop\hadoop-3.2.2\bin -Dhadoop.home.dir=C:\hadoop\hadoop-3.2.2 -Dspark.master=local[*]"
     * </pre>
     *
     * @param args 命令行参数
     * @throws Exception UDF 初始化、执行或结果写出失败时抛出
     */
    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            args = defaultDebugArgs();
            System.out.println("RAHA_UDF_DEFAULT_ARGS="
                    + java.util.Arrays.toString(args));
        }
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException(
                    "用法：RahaUdfDriverApp <functionName> <request|@requestFile> [outputJsonPath]");
        }
        String functionName = args[FUNCTION_ARG_INDEX];
        String request = readRequest(args[REQUEST_ARG_INDEX]);
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
            if (args.length > OUTPUT_ARG_INDEX
                    && args[OUTPUT_ARG_INDEX] != null
                    && !args[OUTPUT_ARG_INDEX].trim().isEmpty()) {
                Path outputPath = Paths.get(args[OUTPUT_ARG_INDEX]);
                if (outputPath.getParent() != null) {
                    Files.createDirectories(outputPath.getParent());
                }
                Files.write(outputPath, json.getBytes(StandardCharsets.UTF_8));
                System.out.println("RAHA_UDF_RESULT_JSON=" + outputPath);
            }
            System.out.println(json);
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
        Path runRoot = defaultDebugRunRoot();
        return new String[] {
                "COLLECT",
                "@" + runRoot.resolve("requests/collect-local.json"),
                runRoot.resolve("outputs/collect-local-result.json").toString()
        };
    }

    /**
     * 构造默认训练参数。
     *
     * @return 可直接传给 {@link #main(String[])} 的训练参数
     */
    public static String[] defaultTrainArgs() {
        Path runRoot = defaultDebugRunRoot();
        return new String[] {
                "TRAIN",
                "@" + runRoot.resolve("requests/train-local.json"),
                runRoot.resolve("outputs/train-local-result.json").toString()
        };
    }

    /**
     * 构造默认检测参数。
     *
     * @return 可直接传给 {@link #main(String[])} 的检测参数
     */
    public static String[] defaultDetectArgs() {
        Path runRoot = defaultDebugRunRoot();
        return new String[] {
                "DETECT",
                "@" + runRoot.resolve("requests/detect-local-limit450.json"),
                runRoot.resolve("outputs/detect-local-limit450-result.json")
                        .toString()
        };
    }

    private static String[] defaultDebugArgs() {
        String functionName = System.getProperty(DEBUG_FUNCTION_PROPERTY,
                "COLLECT");
        String normalized = normalize(functionName);
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

    private static Path defaultDebugRunRoot() {
        String configured = System.getProperty(DEBUG_RUN_ROOT_PROPERTY,
                DEFAULT_DEBUG_RUN_ROOT);
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
        throw new IllegalArgumentException("不支持的 Raha UDF 函数：" + functionName);
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
        throw new IllegalArgumentException("不支持的 Raha UDF 函数：" + functionName);
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

    private static String readRequest(String argument) throws IOException {
        if (argument != null && argument.startsWith("@")) {
            byte[] bytes = Files.readAllBytes(Paths.get(argument.substring(1)));
            return new String(bytes, StandardCharsets.UTF_8).trim();
        }
        return argument;
    }

    private static String normalize(String functionName) {
        return functionName == null ? ""
                : functionName.trim().toUpperCase(Locale.ROOT);
    }
}
