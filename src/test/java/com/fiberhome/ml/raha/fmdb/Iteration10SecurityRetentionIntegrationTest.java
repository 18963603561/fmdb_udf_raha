package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.audit.InMemoryRahaAuditWriter;
import com.fiberhome.ml.raha.audit.RahaAuditAction;
import com.fiberhome.ml.raha.audit.RahaAuditService;
import com.fiberhome.ml.raha.audit.RahaAuditStatus;
import com.fiberhome.ml.raha.audit.SparkSqlRahaAuditWriter;
import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.retention.FmdbRetentionCleanupService;
import com.fiberhome.ml.raha.retention.RetentionCleanupResult;
import com.fiberhome.ml.raha.retention.RetentionTableRule;
import com.fiberhome.ml.raha.security.RahaPermissionAction;
import com.fiberhome.ml.raha.security.RahaPermissionGrant;
import com.fiberhome.ml.raha.security.RahaResourceType;
import com.fiberhome.ml.raha.security.ResultValueProtectionPolicy;
import com.fiberhome.ml.raha.security.RuleBasedRahaPermissionChecker;
import com.fiberhome.ml.raha.testsupport.SparkTestSession;
import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 验证检测结果脱敏、FMDB 审计持久化和过期中间结果清理闭环。
 */
class Iteration10SecurityRetentionIntegrationTest {

    /** 测试检测结果表。 */
    private static final String RESULT_TABLE = "raha_i10_results";
    /** 测试审计表。 */
    private static final String AUDIT_TABLE = "raha_i10_audit";
    /** 测试中间结果表。 */
    private static final String INTERMEDIATE_TABLE = "raha_i10_intermediate";
    /** 每天毫秒数。 */
    private static final long DAY_MILLIS = 24L * 60L * 60L * 1000L;
    /** 当前测试 FMDB 内存网关。 */
    private InMemoryFmdbTableGateway gateway;

    @BeforeEach
    void prepareGateway() {
        SparkSession spark = SparkTestSession.get();
        spark.catalog().dropTempView(RESULT_TABLE);
        spark.catalog().dropTempView(AUDIT_TABLE);
        spark.catalog().dropTempView(INTERMEDIATE_TABLE);
        gateway = new InMemoryFmdbTableGateway(spark);
    }

    @AfterAll
    static void stopSpark() {
        SparkTestSession.stop();
    }

    @Test
    void shouldPersistOnlyHashForSensitiveDetectionValue() {
        SparkSqlFmdbResultWriter writer = new SparkSqlFmdbResultWriter(
                SparkTestSession.get(), gateway, fixedClock(1000L),
                ResultValueProtectionPolicy.hashOnlyForAllColumns());

        writer.writeDetectionResults(RESULT_TABLE, "job-1",
                Collections.singletonList(detection("完整敏感原值")));

        Row stored = gateway.read(RESULT_TABLE).first();
        assertEquals(HashUtils.sha256Hex("完整敏感原值"),
                stored.getAs("value_hash"));
        assertNull(stored.getAs("masked_value"));
    }

    @Test
    void shouldPersistAuditEventIntoFmdbTable() {
        RahaAuditService service = new RahaAuditService(
                new SparkSqlRahaAuditWriter(SparkTestSession.get(), gateway, AUDIT_TABLE),
                fixedClock(2000L));

        service.record("operator", RahaAuditAction.TASK_SUBMIT,
                RahaAuditStatus.SUCCEEDED, RahaResourceType.TASK, "DETECT",
                "dataset", "job-1", "model-v1", "检测任务提交成功");

        assertEquals(1L, gateway.read(AUDIT_TABLE).count());
        Row stored = gateway.read(AUDIT_TABLE).first();
        assertEquals("operator", stored.getAs("actor"));
        assertEquals("TASK_SUBMIT", stored.getAs("action"));
    }

    @Test
    void shouldDeleteOnlyExpiredRowsAndAuditCleanup() {
        long now = 10L * DAY_MILLIS;
        appendIntermediateRows(now);
        InMemoryRahaAuditWriter auditWriter = new InMemoryRahaAuditWriter();
        FmdbRetentionCleanupService service = new FmdbRetentionCleanupService(
                gateway,
                Collections.singletonList(new RetentionTableRule(
                        INTERMEDIATE_TABLE, "updated_at", 5)),
                cleanupPermission(),
                new RahaAuditService(auditWriter, fixedClock(now)),
                fixedClock(now));

        RetentionCleanupResult result = service.cleanup("cleaner", "dataset");

        assertEquals(1L, result.getTotalDeleted());
        assertEquals(1L, gateway.read(INTERMEDIATE_TABLE).count());
        assertEquals("new", gateway.read(INTERMEDIATE_TABLE).first().getAs("record_id"));
        assertEquals(RahaAuditAction.RETENTION_CLEANUP,
                auditWriter.findAll().get(0).getAction());
        assertEquals(RahaAuditStatus.SUCCEEDED,
                auditWriter.findAll().get(0).getStatus());
    }

    private void appendIntermediateRows(long now) {
        StructType schema = new StructType()
                .add("record_id", DataTypes.StringType, false)
                .add("updated_at", DataTypes.LongType, false);
        List<Row> rows = Arrays.asList(
                RowFactory.create("old", DAY_MILLIS),
                RowFactory.create("new", now - DAY_MILLIS));
        gateway.appendIdempotent(INTERMEDIATE_TABLE,
                SparkTestSession.get().createDataFrame(rows, schema),
                Collections.singletonList("record_id"));
    }

    private static RuleBasedRahaPermissionChecker cleanupPermission() {
        Map<String, List<RahaPermissionGrant>> grants =
                new LinkedHashMap<String, List<RahaPermissionGrant>>();
        grants.put("cleaner", Collections.singletonList(new RahaPermissionGrant(
                RahaPermissionAction.CLEANUP, RahaResourceType.INTERMEDIATE_DATA,
                INTERMEDIATE_TABLE, "dataset")));
        return new RuleBasedRahaPermissionChecker("policy-v1", grants);
    }

    private static DetectionResult detection(String value) {
        return new DetectionResult("job-1", "config-v1", "detect-stage",
                new CellCoordinate("dataset", "snapshot-v1", "row-1", "secret"),
                HashUtils.sha256Hex(value), value, true, 0.9d, 0.5d,
                Collections.singletonList("strategy-v1"),
                Collections.singletonMap("reason", "test"), "raha-secret",
                "model-v1", "dictionary-v1", 1000L);
    }

    private static Clock fixedClock(long millis) {
        return Clock.fixed(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
    }
}
