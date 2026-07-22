package com.fiberhome.ml.raha.strategy.plan;

import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 生成策略计划并在同一版本下持久化。
 */
public final class StrategyPlanService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            StrategyPlanService.class);
    /** 策略计划生成器。 */
    private final StrategyPlanGenerator generator;
    /** 策略仓储。 */
    private final StrategyRepository repository;
    /** 提供可测试更新时间的时钟。 */
    private final Clock clock;

    public StrategyPlanService(StrategyPlanGenerator generator,
                               StrategyRepository repository,
                               Clock clock) {
        if (generator == null || repository == null || clock == null) {
            throw new IllegalArgumentException("策略计划服务依赖不能为空");
        }
        this.generator = generator;
        this.repository = repository;
        this.clock = clock;
    }

    public List<StrategyPlan> generateAndSave(RahaDataset dataset,
                                              StrategyConfig config,
                                              ArtifactVersion version) {
        List<StrategyPlan> existing = repository.findPlans(
                dataset.getDatasetId(), dataset.getSnapshotId());
        if (!existing.isEmpty()) {
            Set<String> requestedColumns = requestedColumns(dataset, config);
            List<StrategyPlan> scoped = scopedPlans(existing,
                    requestedColumns);
            if (coveredColumns(scoped).containsAll(requestedColumns)) {
                // 列批任务只能复用当前批字段计划，禁止后续批次继续使用第一批缓存。
                LOGGER.info("复用当前字段范围的已持久化策略计划，datasetId={}，"
                                + "snapshotId={}，requestedColumns={}，planCount={}",
                        dataset.getDatasetId(), dataset.getSnapshotId(),
                        requestedColumns, scoped.size());
                return scoped;
            }
            LOGGER.info("已持久化策略计划未覆盖当前字段范围，重新生成，datasetId={}，"
                            + "snapshotId={}，requestedColumns={}，coveredColumns={}",
                    dataset.getDatasetId(), dataset.getSnapshotId(),
                    requestedColumns, coveredColumns(scoped));
        }
        List<StrategyPlan> plans = generator.generate(dataset, config);
        repository.savePlans(dataset.getDatasetId(), dataset.getSnapshotId(),
                plans, version, clock.millis());
        return plans;
    }

    private static Set<String> requestedColumns(RahaDataset dataset,
                                                StrategyConfig config) {
        Set<String> result = new LinkedHashSet<String>();
        for (ColumnMetadata column : dataset.getColumns()) {
            if (column.isDetectable()
                    && (config.getIncludedColumns().isEmpty()
                    || config.getIncludedColumns().contains(column.getName()))
                    && !config.getExcludedColumns().contains(column.getName())) {
                result.add(column.getName());
            }
        }
        return Collections.unmodifiableSet(result);
    }

    private static List<StrategyPlan> scopedPlans(
            List<StrategyPlan> plans,
            Set<String> requestedColumns) {
        List<StrategyPlan> result = new ArrayList<StrategyPlan>();
        for (StrategyPlan plan : plans) {
            if (requestedColumns.containsAll(plan.getTargetColumns())) {
                result.add(plan);
            }
        }
        return Collections.unmodifiableList(result);
    }

    private static Set<String> coveredColumns(List<StrategyPlan> plans) {
        Set<String> result = new LinkedHashSet<String>();
        for (StrategyPlan plan : plans) {
            result.addAll(plan.getTargetColumns());
        }
        return result;
    }
}
