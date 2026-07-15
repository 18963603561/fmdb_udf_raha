package com.fiberhome.ml.raha.testsupport;

import org.apache.spark.sql.SparkSession;

/**
 * 为 Spark 集成测试提供可复用的本地会话。
 */
public final class SparkTestSession {

    /** 当前测试 Spark 会话。 */
    private static SparkSession sparkSession;

    private SparkTestSession() {
    }

    public static synchronized SparkSession get() {
        if (sparkSession == null || sparkSession.sparkContext().isStopped()) {
            System.setProperty("spark.testing.memory", "2147480000");
            sparkSession = SparkSession.builder()
                    .appName("raha-local-test")
                    .master("local[2]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.driver.host", "127.0.0.1")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.sql.warehouse.dir",
                            System.getProperty("java.io.tmpdir") + "/raha-spark-warehouse")
                    .getOrCreate();
            sparkSession.sparkContext().setLogLevel("WARN");
        }
        return sparkSession;
    }

    public static synchronized void stop() {
        if (sparkSession != null) {
            sparkSession.stop();
            sparkSession = null;
        }
    }
}
