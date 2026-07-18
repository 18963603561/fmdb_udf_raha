package com.fiberhome.ml.raha.strategy;

import com.fiberhome.ml.raha.config.StrategyConfig;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.StrategyRepository;

import java.time.Clock;
import java.util.List;

/**
 * 生成策略计划并在同一版本下持久化。
 */
public final class StrategyPlanService {

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
        List<StrategyPlan> plans = generator.generate(dataset, config);
        repository.savePlans(dataset.getDatasetId(), dataset.getSnapshotId(),
                plans, version, clock.millis());
        return plans;
    }
}
