package com.fiberhome.ml.raha.strategy.plan;

import com.fiberhome.ml.raha.config.dto.StrategyConfig;
import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.port.StrategyRepository;
import java.time.Clock;
import java.util.List;
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
            // 同一快照已有训练冻结计划时直接复用，避免检测阶段生成不同版本的策略定义。
            LOGGER.info("复用已持久化策略计划，datasetId={}，snapshotId={}，planCount={}",
                    dataset.getDatasetId(), dataset.getSnapshotId(), existing.size());
            return existing;
        }
        List<StrategyPlan> plans = generator.generate(dataset, config);
        repository.savePlans(dataset.getDatasetId(), dataset.getSnapshotId(),
                plans, version, clock.millis());
        return plans;
    }
}
