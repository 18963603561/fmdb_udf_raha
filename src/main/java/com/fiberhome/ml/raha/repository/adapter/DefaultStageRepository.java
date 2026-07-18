package com.fiberhome.ml.raha.repository.adapter;

import com.fiberhome.ml.raha.job.domain.RahaStage;
import com.fiberhome.ml.raha.repository.core.ArtifactVersion;
import com.fiberhome.ml.raha.repository.core.RahaRepository;
import com.fiberhome.ml.raha.repository.core.RepositoryKey;
import com.fiberhome.ml.raha.repository.core.RepositoryNamespace;
import com.fiberhome.ml.raha.repository.core.RepositoryRecord;
import com.fiberhome.ml.raha.repository.core.SaveOutcome;
import com.fiberhome.ml.raha.repository.port.StageRepository;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 基于统一仓储实现阶段状态保存和任务内查询。
 */
public final class DefaultStageRepository implements StageRepository {

    /** 统一仓储。 */
    private final RahaRepository repository;

    public DefaultStageRepository(RahaRepository repository) {
        if (repository == null) {
            throw new IllegalArgumentException("统一仓储不能为空");
        }
        this.repository = repository;
    }

    @Override
    public SaveOutcome save(RahaStage stage, ArtifactVersion version, long updatedAt) {
        if (stage == null || version == null) {
            throw new IllegalArgumentException("阶段和结果版本不能为空");
        }
        RepositoryKey key = new RepositoryKey(
                RepositoryNamespace.STAGE, stage.getJobId(), stage.getStageId());
        return repository.save(new RepositoryRecord<RahaStage>(
                key, version, stage.snapshot(), updatedAt));
    }

    @Override
    public List<RahaStage> findByJobId(String jobId) {
        String validatedJobId = ValueUtils.requireNotBlank(jobId, "任务标识");
        List<RepositoryRecord<RahaStage>> records = repository.findByPartition(
                RepositoryNamespace.STAGE, validatedJobId, RahaStage.class);
        List<RahaStage> stages = new ArrayList<RahaStage>(records.size());
        for (RepositoryRecord<RahaStage> record : records) {
            stages.add(record.getPayload().snapshot());
        }
        return Collections.unmodifiableList(stages);
    }
}

