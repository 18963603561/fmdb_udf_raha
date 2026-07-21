package com.fiberhome.ml.raha.detection.service;

import com.fiberhome.ml.raha.config.dto.ModelConfig;
import com.fiberhome.ml.raha.data.domain.CellCoordinate;
import com.fiberhome.ml.raha.data.domain.DetectionResult;
import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.data.type.FeatureType;
import com.fiberhome.ml.raha.data.type.StrategyFamily;
import com.fiberhome.ml.raha.data.type.StrategyStatus;
import com.fiberhome.ml.raha.detection.explanation.DetectionExplanation;
import com.fiberhome.ml.raha.detection.explanation.DetectionExplanationService;
import com.fiberhome.ml.raha.detection.scoring.WeightedRuleScoringRule;
import com.fiberhome.ml.raha.error.RahaErrorCategory;
import com.fiberhome.ml.raha.error.RahaException;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyMetrics;
import com.fiberhome.ml.raha.feature.assembly.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.domain.FeatureDefinition;
import com.fiberhome.ml.raha.feature.domain.FeatureDictionary;
import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.adapter.DefaultDetectionResultRepository;
import com.fiberhome.ml.raha.repository.adapter.InMemoryRahaRepository;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.DetectionResultRepository;
import com.fiberhome.ml.raha.strategy.api.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.api.StrategyTypes;
import com.fiberhome.ml.raha.strategy.domain.StrategyHit;
import com.fiberhome.ml.raha.strategy.plan.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.plan.StrategyPlan;
import com.fiberhome.ml.raha.util.HashUtils;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证加权评分、阈值判断、降级标识、结果持久化和解释反查。
 */
class BasicDetectionServiceTest {

    @Test
    void shouldDetectPersistAndExplainCandidateCell() {
        Fixture fixture = new Fixture();

        DetectionBatchResult batch = fixture.service.detectAndSave(
                "job", "config", "detect-stage", fixture.features,
                Collections.singletonList(fixture.hitForJob("job")),
                new ModelConfig(ClassifierType.WEIGHTED_RULE, 0.5d, false),
                new ArtifactVersion("config", "snapshot", "detect-stage", 1));

        assertEquals(2, batch.getResults().size());
        assertEquals(1L, batch.getMetrics().getErrorCellCount());
        DetectionResult candidate = result(batch.getResults(), "2");
        DetectionResult normal = result(batch.getResults(), "1");
        assertTrue(candidate.isError());
        assertFalse(normal.isError());
        assertEquals("weighted_rule", candidate.getModelName());
        assertEquals("config", candidate.getConfigVersion());
        assertEquals("detect-stage", candidate.getStageId());
        assertEquals(2, fixture.repository.findByJob("job").size());

        DetectionExplanation explanation = new DetectionExplanationService().explain(
                candidate, Collections.singletonList(fixture.plan),
                Collections.singletonList(fixture.hitForJob("job")), fixture.rows.get(1));
        assertEquals(1, explanation.getStrategies().size());
        assertEquals("PVD_TYPE_MISMATCH",
                explanation.getStrategies().get(0).getReasonCode());
        assertEquals("MIXED", explanation.getFeatureSummary().get("valueType"));
    }

    @Test
    void shouldApplyConfigurableFamilyWeight() {
        Fixture fixture = new Fixture();
        Map<StrategyFamily, Double> lowWeights = new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        lowWeights.put(StrategyFamily.PVD, 0.4d);
        ModelConfig lowConfig = new ModelConfig(
                ClassifierType.WEIGHTED_RULE, 0.5d, true, lowWeights, 0.0d);
        DetectionBatchResult low = fixture.service.detectAndSave(
                "low-job", "config", "stage", fixture.features,
                Collections.singletonList(fixture.hitForJob("low-job")), lowConfig,
                new ArtifactVersion("config", "snapshot", "stage", 1));

        Map<StrategyFamily, Double> highWeights = new EnumMap<StrategyFamily, Double>(StrategyFamily.class);
        highWeights.put(StrategyFamily.PVD, 0.8d);
        ModelConfig highConfig = new ModelConfig(
                ClassifierType.WEIGHTED_RULE, 0.5d, true, highWeights, 0.0d);
        DetectionBatchResult high = fixture.service.detectAndSave(
                "high-job", "config", "stage", fixture.features,
                Collections.singletonList(fixture.hitForJob("high-job")), highConfig,
                new ArtifactVersion("config", "snapshot", "stage", 1));

        assertFalse(result(low.getResults(), "2").isError());
        assertTrue(result(high.getResults(), "2").isError());
    }

    @Test
    void shouldMarkFallbackAndRejectUnavailableModelWhenFallbackDisabled() {
        Fixture fixture = new Fixture();
        ModelConfig fallback = new ModelConfig(
                ClassifierType.LOGISTIC_REGRESSION, 0.5d, true);
        DetectionBatchResult result = fixture.service.detectAndSave(
                "fallback-job", "config", "stage", fixture.features,
                Collections.singletonList(fixture.hitForJob("fallback-job")), fallback,
                new ArtifactVersion("config", "snapshot", "stage", 1));
        assertEquals("fallback_weighted_rule",
                result(result.getResults(), "2").getModelName());

        RahaException exception = assertThrows(RahaException.class, () ->
                fixture.service.detectAndSave("failed-job", "config", "stage", fixture.features,
                        Collections.singletonList(fixture.hitForJob("failed-job")),
                        new ModelConfig(ClassifierType.LOGISTIC_REGRESSION, 0.5d, false),
                        new ArtifactVersion("config", "snapshot", "stage", 1)));
        assertEquals(RahaErrorCategory.DETECTION, exception.getCategory());
    }

    private static DetectionResult result(List<DetectionResult> results, String rowId) {
        for (DetectionResult result : results) {
            if (result.getCoordinate().getRowId().equals(rowId)) {
                return result;
            }
        }
        return null;
    }

    private static final class Fixture {
        /** 测试策略计划。 */
        private final StrategyPlan plan;
        /** 测试策略命中。 */
        private final StrategyHit hit;
        /** 测试特征行。 */
        private final List<SparseFeatureRow> rows;
        /** 测试特征结果。 */
        private final FeatureAssemblyResult features;
        /** 测试检测结果仓储。 */
        private final DetectionResultRepository repository;
        /** 测试基础检测服务。 */
        private final BasicDetectionService service;

        private Fixture() {
            Map<String, String> configuration = new LinkedHashMap<String, String>();
            configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE,
                    StrategyTypes.PVD_TYPE_FORMAT);
            configuration.put(StrategyConfigurationKeys.MINORITY_RATIO, "0.1");
            configuration.put(StrategyConfigurationKeys.FORMAT_TYPE, "AUTO");
            configuration.put(StrategyConfigurationKeys.FORMAT_MIN_RATIO, "0.8");
            List<String> columns = Collections.singletonList("code");
            this.plan = new StrategyPlan(StrategyIdentityGenerator.strategyId(
                    StrategyFamily.PVD, columns, configuration), StrategyFamily.PVD,
                    columns, configuration, 1, StrategyStatus.PLANNED);
            CellCoordinate normal = new CellCoordinate("dataset", "snapshot", "1", "code");
            CellCoordinate candidate = new CellCoordinate("dataset", "snapshot", "2", "code");
            this.hit = new StrategyHit("job", "strategy-stage", plan.getStrategyId(),
                    StrategyFamily.PVD, candidate, HashUtils.md5Hex("A1#"),
                    "PVD_TYPE_MISMATCH", Collections.singletonMap("actualType", "MIXED"),
                    1.0d, 1L, StrategyStatus.SUCCEEDED);
            Map<Integer, FeatureDefinition> definitions = new LinkedHashMap<Integer, FeatureDefinition>();
            definitions.put(0, new FeatureDefinition(0, "strategy.pvd.type.hit",
                    FeatureType.BINARY, plan.getStrategyId(), 0.0d));
            definitions.put(1, new FeatureDefinition(1,
                    "context.column.frequency_bucket.rare",
                    FeatureType.BINARY, "value_context", 0.0d));
            FeatureDictionary dictionary = new FeatureDictionary(
                    "dictionary", "code", definitions, 1L);
            this.rows = Arrays.asList(
                    new SparseFeatureRow(normal.toCellId(), "code", normal,
                            HashUtils.md5Hex("ABC"), null, "dictionary",
                            Collections.<Integer, Double>emptyMap(),
                            Collections.singletonMap("valueType", "LETTER")),
                    new SparseFeatureRow(candidate.toCellId(), "code", candidate,
                            HashUtils.md5Hex("A1#"), null, "dictionary",
                            map(0, 1.0d, 1, 1.0d),
                            Collections.singletonMap("valueType", "MIXED")));
            this.features = new FeatureAssemblyResult(
                    Collections.singletonMap("code", dictionary), rows,
                    new FeatureAssemblyMetrics(2L, 2L, 2L, 0L));
            this.repository = new DefaultDetectionResultRepository(new InMemoryRahaRepository());
            Clock clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC);
            this.service = new BasicDetectionService(
                    new WeightedRuleScoringRule(), repository, clock);
        }

        private StrategyHit hitForJob(String jobId) {
            return new StrategyHit(jobId, hit.getStageId(), hit.getStrategyId(),
                    hit.getStrategyFamily(), hit.getCoordinate(), hit.getValueHash(),
                    hit.getReasonCode(), hit.getReasonDetails(), hit.getStrategyScore(),
                    hit.getRuntimeMillis(), hit.getStatus());
        }
    }

    private static Map<Integer, Double> map(int firstKey,
                                            double firstValue,
                                            int secondKey,
                                            double secondValue) {
        Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
        values.put(firstKey, firstValue);
        values.put(secondKey, secondValue);
        return values;
    }
}
