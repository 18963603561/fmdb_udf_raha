package com.fiberhome.ml.raha.feature;

import com.fiberhome.ml.raha.config.FeatureConfig;
import com.fiberhome.ml.raha.data.RahaDataset;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.FeatureRepository;
import com.fiberhome.ml.raha.strategy.StrategyHit;
import com.fiberhome.ml.raha.strategy.StrategyPlan;

import java.time.Clock;
import java.util.List;

/**
 * 组装并版本化保存单元格特征。
 */
public final class FeatureService {

    /** 特征组装器。 */
    private final FeatureAssembler assembler;
    /** 特征仓储。 */
    private final FeatureRepository repository;
    /** 提供可测试更新时间的时钟。 */
    private final Clock clock;

    public FeatureService(FeatureAssembler assembler,
                          FeatureRepository repository,
                          Clock clock) {
        if (assembler == null || repository == null || clock == null) {
            throw new IllegalArgumentException("特征服务依赖不能为空");
        }
        this.assembler = assembler;
        this.repository = repository;
        this.clock = clock;
    }

    public FeatureAssemblyResult assembleAndSave(String jobId,
                                                 RahaDataset dataset,
                                                 List<StrategyPlan> plans,
                                                 List<StrategyHit> hits,
                                                 FeatureConfig config,
                                                 ArtifactVersion version) {
        FeatureAssemblyResult result = assembler.assemble(dataset, plans, hits, config);
        repository.save(jobId, result, version, clock.millis());
        return result;
    }
}
