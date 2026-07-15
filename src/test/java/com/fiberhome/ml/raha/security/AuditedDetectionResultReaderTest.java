package com.fiberhome.ml.raha.security;

import com.fiberhome.ml.raha.audit.InMemoryRahaAuditWriter;
import com.fiberhome.ml.raha.audit.RahaAuditAction;
import com.fiberhome.ml.raha.audit.RahaAuditService;
import com.fiberhome.ml.raha.audit.RahaAuditStatus;
import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.DetectionResultRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证检测结果读取权限、数据集隔离和访问审计。
 */
class AuditedDetectionResultReaderTest {

    @Test
    void shouldAuthorizeAndAuditResultAccess() {
        DetectionResultRepository repository = repositoryWithResult();
        InMemoryRahaAuditWriter auditWriter = new InMemoryRahaAuditWriter();
        AuditedDetectionResultReader reader = new AuditedDetectionResultReader(
                repository, checker(true), audit(auditWriter), "raha_results");

        List<DetectionResult> results = reader.findByJob(
                "reader", "dataset", "job-1");

        assertEquals(1, results.size());
        assertEquals(RahaAuditAction.RESULT_ACCESS,
                auditWriter.findAll().get(0).getAction());
        assertEquals(RahaAuditStatus.SUCCEEDED,
                auditWriter.findAll().get(0).getStatus());
    }

    @Test
    void shouldRejectAndAuditUnauthorizedResultAccess() {
        InMemoryRahaAuditWriter auditWriter = new InMemoryRahaAuditWriter();
        AuditedDetectionResultReader reader = new AuditedDetectionResultReader(
                repositoryWithResult(), checker(false), audit(auditWriter),
                "raha_results");

        assertThrows(RahaAccessDeniedException.class,
                () -> reader.findByJob("reader", "dataset", "job-1"));

        assertEquals(1, auditWriter.findAll().size());
        assertEquals(RahaAuditStatus.DENIED,
                auditWriter.findAll().get(0).getStatus());
    }

    private static DetectionResultRepository repositoryWithResult() {
        DetectionResultRepository repository = new DefaultDetectionResultRepository(
                new InMemoryRahaRepository());
        repository.saveAll("job-1", Collections.singletonList(result()),
                new ArtifactVersion("config-v1", "snapshot-v1", "persist", 1),
                1000L);
        return repository;
    }

    private static RuleBasedRahaPermissionChecker checker(boolean grantAccess) {
        Map<String, List<RahaPermissionGrant>> grants =
                new LinkedHashMap<String, List<RahaPermissionGrant>>();
        if (grantAccess) {
            grants.put("reader", Collections.singletonList(new RahaPermissionGrant(
                    RahaPermissionAction.READ, RahaResourceType.RESULT_DATA,
                    "raha_results", "dataset")));
        }
        return new RuleBasedRahaPermissionChecker("policy-v1", grants);
    }

    private static RahaAuditService audit(InMemoryRahaAuditWriter writer) {
        return new RahaAuditService(writer,
                Clock.fixed(Instant.ofEpochMilli(2000L), ZoneOffset.UTC));
    }

    private static DetectionResult result() {
        return new DetectionResult("job-1", "config-v1", "persist",
                new CellCoordinate("dataset", "snapshot-v1", "row-1", "code"),
                HashUtils.sha256Hex("value"), "***", true, 0.9d, 0.5d,
                Collections.singletonList("strategy-v1"),
                Collections.singletonMap("reason", "test"), "raha-code",
                "model-v1", "dictionary-v1", 1000L);
    }
}
