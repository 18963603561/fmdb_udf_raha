package com.fiberhome.ml.raha.udf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * 三个 Raha 检测 UDF 的公共 GenericUDF 基类。
 */
public abstract class AbstractRahaGenericUdf extends GenericUDF {

    /** 日志记录器。 */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractRahaGenericUdf.class);
    /** 当前 UDF 运行使用的 Spark SQL 上下文。 */
    private SQLContext sqlContext;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments == null || arguments.length != 1) {
            throw new UDFArgumentLengthException(
                    functionName() + " 需要且仅需要 1 个字符串参数");
        }
        sqlContext = resolveSqlContext();
        sqlContext.setConf("spark.sql.udf.local.mode", "true");
        List<String> fieldNames = new ArrayList<String>();
        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        for (RahaUdfField field : fields()) {
            fieldNames.add(field.getName());
            fieldInspectors.add(field.objectInspector());
        }
        return ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getStandardStructObjectInspector(
                        fieldNames, fieldInspectors));
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        long startedAt = System.currentTimeMillis();
        try {
            Object raw = arguments[0].get();
            if (raw == null) {
                throw new RahaUdfException("INVALID_ARGUMENT",
                        functionName() + " 入参不能为空");
            }
            RahaDetectionUdfService service =
                    RahaUdfRuntime.service(sqlContext);
            RahaUdfRows rows = doEvaluate(String.valueOf(raw), service);
            LOGGER.info("{} 执行完成，rowCount={}，costMillis={}",
                    functionName(), rows.getRows().size(),
                    System.currentTimeMillis() - startedAt);
            return toStructRows(rows);
        } catch (Exception exception) {
            LOGGER.error("{} 执行失败", functionName(), exception);
            return toStructRows(failedRows(exception));
        }
    }

    @Override
    public ObjectInspector initializeAndFoldConstants(ObjectInspector[] arguments)
            throws UDFArgumentException {
        return initialize(arguments);
    }

    @Override
    public String getDisplayString(String[] children) {
        return functionName();
    }

    protected abstract String functionName();

    protected abstract List<RahaUdfField> fields();

    protected abstract RahaUdfRows doEvaluate(String argument,
                                              RahaDetectionUdfService service);

    private List<Object[]> toStructRows(RahaUdfRows result) {
        List<Object[]> rows = new ArrayList<Object[]>();
        for (Map<String, Object> row : result.getRows()) {
            Object[] values = new Object[fields().size()];
            for (int index = 0; index < fields().size(); index++) {
                RahaUdfField field = fields().get(index);
                values[index] = convertValue(row.get(field.getName()),
                        field.getType());
            }
            rows.add(values);
        }
        return rows;
    }

    private RahaUdfRows failedRows(Exception exception) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("status", "FAILED");
        if (exception instanceof RahaUdfException) {
            RahaUdfException udfException = (RahaUdfException) exception;
            row.put("errorCode", udfException.getErrorCode());
            row.put("errorMessage", udfException.getMessage());
        } else {
            row.put("errorCode", "UNEXPECTED_ERROR");
            row.put("errorMessage", exception.getMessage());
        }
        row.put("createdAt", System.currentTimeMillis());
        return RahaUdfRows.single(row);
    }

    private static Object convertValue(Object value, RahaUdfFieldType type) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case LONG:
                return value instanceof Number
                        ? Long.valueOf(((Number) value).longValue())
                        : Long.valueOf(String.valueOf(value));
            case INT:
                return value instanceof Number
                        ? Integer.valueOf(((Number) value).intValue())
                        : Integer.valueOf(String.valueOf(value));
            case DOUBLE:
                return value instanceof Number
                        ? Double.valueOf(((Number) value).doubleValue())
                        : Double.valueOf(String.valueOf(value));
            case BOOLEAN:
                return value instanceof Boolean ? value
                        : Boolean.valueOf(String.valueOf(value));
            case STRING:
            default:
                return String.valueOf(value);
        }
    }

    private static SQLContext resolveSqlContext() {
        Option<SparkSession> active = SparkSession.getActiveSession();
        if (active != null && active.isDefined()) {
            return active.get().sqlContext();
        }
        Option<SparkSession> defaultSession = SparkSession.getDefaultSession();
        if (defaultSession != null && defaultSession.isDefined()) {
            // Spark SQL 可能在执行线程中初始化常量 UDF，此时只有默认会话可用。
            return defaultSession.get().sqlContext();
        }
        throw new IllegalStateException(
                "Raha UDF 需要当前线程存在活动 SparkSession");
    }
}
