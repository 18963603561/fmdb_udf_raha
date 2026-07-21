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

    private RahaUdfDriverApp() {
    }

    public static void main(String[] args) throws Exception {
        if (args == null || args.length < 2) {
            throw new IllegalArgumentException(
                    "用法：RahaUdfDriverApp <functionName> <request|@requestFile> [outputJsonPath]");
        }
        String functionName = args[FUNCTION_ARG_INDEX];
        String request = readRequest(args[REQUEST_ARG_INDEX]);
        SparkSession spark = SparkSession.builder()
                .appName("RahaUdfDriverApp-" + functionName)
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
