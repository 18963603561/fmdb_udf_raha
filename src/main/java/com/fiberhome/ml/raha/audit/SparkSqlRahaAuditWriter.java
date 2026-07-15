package com.fiberhome.ml.raha.audit;

import com.fiberhome.ml.raha.fmdb.FmdbTableGateway;
import com.fiberhome.ml.raha.fmdb.SparkSqlFmdbTableGateway;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * 将安全审计事件转换为稳定模式后幂等写入 FMDB 审计表。
 */
public final class SparkSqlRahaAuditWriter implements RahaAuditWriter {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            SparkSqlRahaAuditWriter.class);
    /** 审计表稳定模式。 */
    private static final StructType AUDIT_SCHEMA = DataTypes.createStructType(
            new StructField[]{
                    field("event_id", DataTypes.StringType, false),
                    field("actor", DataTypes.StringType, false),
                    field("action", DataTypes.StringType, false),
                    field("status", DataTypes.StringType, false),
                    field("resource_type", DataTypes.StringType, false),
                    field("resource_name", DataTypes.StringType, false),
                    field("dataset_id", DataTypes.StringType, false),
                    field("job_id", DataTypes.StringType, true),
                    field("model_version", DataTypes.StringType, true),
                    field("summary", DataTypes.StringType, false),
                    field("occurred_at", DataTypes.LongType, false)
            });
    /** Spark 会话。 */
    private final SparkSession sparkSession;
    /** FMDB 表网关。 */
    private final FmdbTableGateway tableGateway;
    /** FMDB 审计表名。 */
    private final String auditTable;

    public SparkSqlRahaAuditWriter(SparkSession sparkSession,
                                   FmdbTableGateway tableGateway,
                                   String auditTable) {
        if (sparkSession == null || tableGateway == null) {
            throw new IllegalArgumentException("FMDB 审计写入器依赖不能为空");
        }
        this.sparkSession = sparkSession;
        this.tableGateway = tableGateway;
        this.auditTable = SparkSqlFmdbTableGateway.validateTableName(auditTable);
    }

    @Override
    public void write(RahaAuditEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("审计事件不能为空");
        }
        LOGGER.info("开始写入 FMDB 审计事件，eventId={}，action={}，status={}，tableName={}",
                event.getEventId(), event.getAction(), event.getStatus(), auditTable);
        try {
            Row row = RowFactory.create(event.getEventId(), event.getActor(),
                    event.getAction().name(), event.getStatus().name(),
                    event.getResourceType().name(), event.getResourceName(),
                    event.getDatasetId(), event.getJobId(), event.getModelVersion(),
                    event.getSummary(), event.getOccurredAt());
            Dataset<Row> frame = sparkSession.createDataFrame(
                    Collections.singletonList(row), AUDIT_SCHEMA);
            tableGateway.appendIdempotent(auditTable, frame,
                    Collections.singletonList("event_id"));
        } catch (RuntimeException exception) {
            // 审计写入失败必须保留事件、动作和目标表上下文，并向上游暴露失败。
            LOGGER.error("FMDB 审计事件写入失败，eventId={}，action={}，tableName={}",
                    event.getEventId(), event.getAction(), auditTable, exception);
            throw exception;
        }
    }

    private static StructField field(String name,
                                     org.apache.spark.sql.types.DataType type,
                                     boolean nullable) {
        return DataTypes.createStructField(name, type, nullable);
    }
}
