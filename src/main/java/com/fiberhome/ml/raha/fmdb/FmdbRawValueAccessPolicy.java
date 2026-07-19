package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 控制原始值字段访问，并记录授权主体和访问用途，避免普通查询泄露敏感原文。
 */
public final class FmdbRawValueAccessPolicy {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            FmdbRawValueAccessPolicy.class);
    /** 授权主体。 */
    private final String actor;
    /** 是否允许访问所有数据集。 */
    private final boolean allowAll;
    /** 被授权的数据集集合。 */
    private final Set<String> datasets;

    private FmdbRawValueAccessPolicy(String actor, boolean allowAll,
                                     Collection<String> datasets) {
        this.actor = ValueUtils.requireNotBlank(actor, "原始值访问主体");
        this.allowAll = allowAll;
        Set<String> copied = new LinkedHashSet<String>();
        if (datasets != null) {
            for (String dataset : datasets) {
                copied.add(ValueUtils.requireNotBlank(dataset, "授权数据集"));
            }
        }
        this.datasets = Collections.unmodifiableSet(copied);
    }

    /** 创建拒绝所有原始值读取的策略。 */
    public static FmdbRawValueAccessPolicy denyAll(String actor) {
        return new FmdbRawValueAccessPolicy(actor, false, null);
    }

    /** 创建只允许指定数据集的策略。 */
    public static FmdbRawValueAccessPolicy forDatasets(String actor,
                                                       Collection<String> datasets) {
        return new FmdbRawValueAccessPolicy(actor, false, datasets);
    }

    /** 创建供内部可信服务使用的全数据集策略。 */
    public static FmdbRawValueAccessPolicy allowAllInternal() {
        return new FmdbRawValueAccessPolicy("raha-internal", true, null);
    }

    /** 校验原始值字段访问权限，并记录审计上下文。 */
    public void requireRead(String datasetId, String purpose) {
        String dataset = ValueUtils.requireNotBlank(datasetId, "原始值数据集");
        String reason = ValueUtils.requireNotBlank(purpose, "原始值访问用途");
        if (!allowAll && !datasets.contains(dataset)) {
            LOGGER.warn("拒绝原始值访问，actor={}，datasetId={}，purpose={}",
                    actor, dataset, reason);
            throw new SecurityException("当前主体无权读取数据集原始值：" + dataset);
        }
        LOGGER.info("原始值访问审计，actor={}，datasetId={}，purpose={}",
                actor, dataset, reason);
    }

    public String getActor() {
        return actor;
    }
}
