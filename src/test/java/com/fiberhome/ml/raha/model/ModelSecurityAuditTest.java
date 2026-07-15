package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.audit.InMemoryRahaAuditWriter;
import com.fiberhome.ml.raha.audit.RahaAuditAction;
import com.fiberhome.ml.raha.audit.RahaAuditService;
import com.fiberhome.ml.raha.audit.RahaAuditStatus;
import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.ModelStatus;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DefaultModelMetadataRepository;
import com.fiberhome.ml.raha.repository.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
import com.fiberhome.ml.raha.security.RahaAccessDeniedException;
import com.fiberhome.ml.raha.security.RahaPermissionAction;
import com.fiberhome.ml.raha.security.RahaPermissionGrant;
import com.fiberhome.ml.raha.security.RahaResourceType;
import com.fiberhome.ml.raha.security.RuleBasedRahaPermissionChecker;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证列级模型发布、停用权限和审计事件完整性。
 */
class ModelSecurityAuditTest {

    @Test
    void shouldAuthorizeAndAuditPublishAndDisable() {
        ModelMetadataRepository repository = repository();
        InMemoryRahaAuditWriter auditWriter = new InMemoryRahaAuditWriter();
        ModelReleaseManager manager = manager(repository, auditWriter,
                grants("publisher", Arrays.asList(
                        grant(RahaPermissionAction.PUBLISH),
                        grant(RahaPermissionAction.DISABLE))));
        manager.markCandidate(model(), version("candidate"));

        manager.publish("dataset", "code", "model-v1",
                version("publish"), "publisher");
        manager.disable("dataset", "code", "model-v1",
                version("disable"), "publisher");

        assertEquals(ModelStatus.DISABLED, repository.find(
                "dataset", "code", "model-v1").get().getStatus());
        assertEquals(2, auditWriter.findAll().size());
        assertEquals(RahaAuditAction.MODEL_PUBLISH,
                auditWriter.findAll().get(0).getAction());
        assertEquals(RahaAuditAction.MODEL_DISABLE,
                auditWriter.findAll().get(1).getAction());
        assertEquals(RahaAuditStatus.SUCCEEDED,
                auditWriter.findAll().get(1).getStatus());
    }

    @Test
    void shouldRejectAndAuditUnauthorizedPublish() {
        ModelMetadataRepository repository = repository();
        InMemoryRahaAuditWriter auditWriter = new InMemoryRahaAuditWriter();
        ModelReleaseManager manager = manager(repository, auditWriter,
                Collections.<String, List<RahaPermissionGrant>>emptyMap());
        manager.markCandidate(model(), version("candidate"));

        assertThrows(RahaAccessDeniedException.class,
                () -> manager.publish("dataset", "code", "model-v1",
                        version("publish"), "visitor"));

        assertEquals(ModelStatus.CANDIDATE, repository.find(
                "dataset", "code", "model-v1").get().getStatus());
        assertEquals(1, auditWriter.findAll().size());
        assertEquals(RahaAuditStatus.DENIED,
                auditWriter.findAll().get(0).getStatus());
    }

    private static ModelReleaseManager manager(
            ModelMetadataRepository repository,
            InMemoryRahaAuditWriter auditWriter,
            Map<String, List<RahaPermissionGrant>> grants) {
        Clock clock = Clock.fixed(Instant.ofEpochMilli(3000L), ZoneOffset.UTC);
        return new ModelReleaseManager(repository, clock,
                new RuleBasedRahaPermissionChecker("policy-v1", grants),
                new RahaAuditService(auditWriter, clock));
    }

    private static ModelMetadataRepository repository() {
        return new DefaultModelMetadataRepository(new InMemoryRahaRepository());
    }

    private static Map<String, List<RahaPermissionGrant>> grants(
            String actor,
            List<RahaPermissionGrant> actorGrants) {
        Map<String, List<RahaPermissionGrant>> values =
                new LinkedHashMap<String, List<RahaPermissionGrant>>();
        values.put(actor, actorGrants);
        return values;
    }

    private static RahaPermissionGrant grant(RahaPermissionAction action) {
        return new RahaPermissionGrant(action, RahaResourceType.MODEL,
                "code:model-v1", "dataset");
    }

    private static RahaColumnModel model() {
        return new RahaColumnModel("raha-code", "model-v1", "dataset", "code",
                "schema-v1", ClassifierType.LOGISTIC_REGRESSION, "dictionary-v1",
                "plan-v1", 0.5d, "fmdb://models/model-v1", ModelStatus.DRAFT,
                Collections.<String, Double>emptyMap(), 1000L);
    }

    private static ArtifactVersion version(String stageId) {
        return new ArtifactVersion("config-v1", "snapshot-v1", stageId, 1);
    }
}
