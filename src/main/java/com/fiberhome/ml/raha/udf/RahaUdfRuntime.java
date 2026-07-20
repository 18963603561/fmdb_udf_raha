package com.fiberhome.ml.raha.udf;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * 管理 UDF 进程内共享的 Raha 检测服务实例。
 */
public final class RahaUdfRuntime {

    /** 默认服务实例，避免每一行 UDF 调用都重新装配完整工作流。 */
    private static volatile RahaDetectionUdfService service;

    private RahaUdfRuntime() {
    }

    public static RahaDetectionUdfService service(SQLContext sqlContext) {
        if (sqlContext == null) {
            throw new IllegalArgumentException("Spark SQL 上下文不能为空");
        }
        RahaDetectionUdfService current = service;
        if (current == null) {
            synchronized (RahaUdfRuntime.class) {
                current = service;
                if (current == null) {
                    SparkSession spark = sqlContext.sparkSession();
                    current = RahaDetectionUdfService.create(spark);
                    service = current;
                }
            }
        }
        return current;
    }
}
