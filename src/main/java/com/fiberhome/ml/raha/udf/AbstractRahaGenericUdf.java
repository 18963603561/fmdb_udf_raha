package com.fiberhome.ml.raha.udf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.spark.TaskContext;
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
            RahaUdfRows rows = evaluateOnDriverThread(String.valueOf(raw));
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

    private RahaUdfRows evaluateOnDriverThread(final String argument)
            throws Exception {
        if (TaskContext.get() == null) {
            RahaDetectionUdfService service =
                    RahaUdfRuntime.service(sqlContext);
            return doEvaluate(argument, service);
        }
        FutureTask<RahaUdfRows> task = new FutureTask<RahaUdfRows>(
                new Callable<RahaUdfRows>() {
                    @Override
                    public RahaUdfRows call() {
                        // UDF 的完整工作流会再次发起 Spark SQL 查询；
                        // 独立线程没有 TaskContext，可避免在 executor task 内嵌套驱动 Spark。
                        RahaDetectionUdfService service =
                                RahaUdfRuntime.service(sqlContext);
                        return doEvaluate(argument, service);
                    }
                });
        Thread thread = new Thread(task,
                "raha-udf-driver-thread-" + functionName());
        thread.setDaemon(true);
        thread.start();
        try {
            return task.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw exception;
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

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
        try {
            SparkSession session = SparkSession.builder().getOrCreate();
            if (session != null) {
                // Spark SQL CLI 可能在任务线程初始化 GenericUDF，兜底复用当前 JVM 的 Spark 会话。
                LOGGER.warn("当前线程没有活动或默认 SparkSession，已通过 SparkSession.builder().getOrCreate() 获取会话");
                return session.sqlContext();
            }
        } catch (Exception exception) {
            LOGGER.error("Raha UDF 获取 SparkSession 失败", exception);
            SQLContext taskLocalContext = resolveSqlContextFromLocalTask(exception);
            if (taskLocalContext != null) {
                return taskLocalContext;
            }
        }
        throw new IllegalStateException(
                "Raha UDF 需要当前线程存在活动 SparkSession");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SQLContext resolveSqlContextFromLocalTask(Exception cause) {
        TaskContext currentTask = TaskContext.get();
        if (currentTask == null) {
            return null;
        }
        try {
            Class<?> moduleClass = Class.forName("org.apache.spark.TaskContext$");
            Object module = moduleClass.getField("MODULE$").get(null);
            java.lang.reflect.Field field =
                    moduleClass.getDeclaredField("taskContext");
            field.setAccessible(true);
            ThreadLocal taskContextThreadLocal =
                    (ThreadLocal) field.get(module);
            Object previous = taskContextThreadLocal.get();
            try {
                // Spark SQL 无来源 SELECT 会在本地 task 线程初始化 UDF；
                // 这里短暂清除 TaskContext，仅用于本地验证时复用 driver 会话。
                taskContextThreadLocal.remove();
                SparkSession session = SparkSession.builder().getOrCreate();
                if (session == null) {
                    return null;
                }
                LOGGER.warn("Raha UDF 已通过本地 task 线程兼容逻辑复用 SparkSession，仅适用于 spark-sql 本地执行验证", cause);
                return session.sqlContext();
            } finally {
                if (previous == null) {
                    taskContextThreadLocal.remove();
                } else {
                    taskContextThreadLocal.set(previous);
                }
            }
        } catch (Exception reflectionException) {
            LOGGER.warn("Raha UDF 本地 task 线程兼容逻辑未生效", reflectionException);
            return null;
        }
    }
}
