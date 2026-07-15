package com.fiberhome.ml.raha.repository;

import com.fiberhome.ml.raha.data.CellCoordinate;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.data.StrategyStatus;
import com.fiberhome.ml.raha.strategy.StrategyConfigurationKeys;
import com.fiberhome.ml.raha.strategy.StrategyExecutionResult;
import com.fiberhome.ml.raha.strategy.StrategyHit;
import com.fiberhome.ml.raha.strategy.StrategyIdentityGenerator;
import com.fiberhome.ml.raha.strategy.StrategyPlan;
import com.fiberhome.ml.raha.strategy.StrategyRunSummary;
import com.fiberhome.ml.raha.strategy.StrategyTypes;
import com.fiberhome.ml.raha.util.HashUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 验证策略计划、命中和摘要的事务保存及稳定去重。
 */
class StrategyRepositoryTest {

    @Test
    void shouldPersistAndDeduplicateStrategyArtifacts() {
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        StrategyRepository repository = new DefaultStrategyRepository(storage);
        StrategyPlan plan = plan();
        ArtifactVersion version = new ArtifactVersion(
                "config", "snapshot", "strategy-stage", 1);
        StrategyExecutionResult execution = execution(plan);

        repository.savePlans("dataset", "snapshot",
                Collections.singletonList(plan), version, 1L);
        repository.saveExecution(execution, version, 1L);
        repository.savePlans("dataset", "snapshot",
                Collections.singletonList(plan), version, 2L);
        repository.saveExecution(execution, version, 2L);

        assertEquals(3, storage.size());
        assertEquals(1, repository.findPlans("dataset", "snapshot").size());
        assertEquals(1, repository.findHits("job").size());
        assertEquals(1, repository.findSummaries("job").size());
        assertEquals(plan.getConfigurationHash(),
                repository.findSummaries("job").get(0).getConfigurationHash());
    }

    @Test
    void shouldReadStrategyPlansByPriority() {
        InMemoryRahaRepository storage = new InMemoryRahaRepository();
        StrategyRepository repository = new DefaultStrategyRepository(storage);
        StrategyPlan later = plan(StrategyTypes.PVD_TYPE_FORMAT, 20);
        StrategyPlan earlier = plan(StrategyTypes.PVD_NULL_PLACEHOLDER, 1);

        repository.savePlans("dataset", "snapshot",
                java.util.Arrays.asList(later, earlier),
                new ArtifactVersion("config", "snapshot", "stage", 1), 1L);

        assertEquals(earlier.getStrategyId(),
                repository.findPlans("dataset", "snapshot").get(0).getStrategyId());
    }

    private static StrategyPlan plan() {
        return plan(StrategyTypes.PVD_NULL_PLACEHOLDER, 1);
    }

    private static StrategyPlan plan(String strategyType, int priority) {
        Map<String, String> configuration = new LinkedHashMap<String, String>();
        configuration.put(StrategyConfigurationKeys.STRATEGY_TYPE, strategyType);
        if (StrategyTypes.PVD_NULL_PLACEHOLDER.equals(strategyType)) {
            configuration.put(StrategyConfigurationKeys.PLACEHOLDERS, "N/A");
        } else {
            configuration.put(StrategyConfigurationKeys.MINORITY_RATIO, "0.1");
            configuration.put(StrategyConfigurationKeys.FORMAT_TYPE, "AUTO");
            configuration.put(StrategyConfigurationKeys.FORMAT_MIN_RATIO, "0.8");
        }
        List<String> columns = Collections.singletonList("status");
        return new StrategyPlan(StrategyIdentityGenerator.strategyId(
                StrategyFamily.PVD, columns, configuration), StrategyFamily.PVD,
                columns, configuration, priority, StrategyStatus.PLANNED);
    }

    private static StrategyExecutionResult execution(StrategyPlan plan) {
        CellCoordinate coordinate = new CellCoordinate("dataset", "snapshot", "1", "status");
        StrategyHit hit = new StrategyHit("job", "stage", plan.getStrategyId(),
                StrategyFamily.PVD, coordinate, HashUtils.sha256Hex("N/A"),
                "PVD_PLACEHOLDER_VALUE", Collections.singletonMap("valueCategory", "placeholder"),
                1.0d, 10L, StrategyStatus.SUCCEEDED);
        StrategyRunSummary summary = new StrategyRunSummary(
                "job", "stage", "snapshot", plan.getStrategyId(),
                plan.getConfigurationHash(), StrategyFamily.PVD, StrategyStatus.SUCCEEDED,
                1L, 1L, 10L, null, null, 1L);
        return new StrategyExecutionResult(summary, Collections.singletonList(hit));
    }
}
