package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.util.ReadableIdUtils;
import com.fiberhome.ml.raha.util.ValueUtils;

/**
 * 表示模型命名使用的可读来源键。
 */
public final class ModelSourceKey {

    /** 可读来源名称，通常是库名和表名。 */
    private final String sourceName;

    private ModelSourceKey(String sourceName) {
        this.sourceName = ReadableIdUtils.normalizeSourceName(sourceName);
    }

    /**
     * 基于来源名称创建模型来源键。
     *
     * @param sourceName 表名或逻辑来源名称
     * @return 模型来源键
     */
    public static ModelSourceKey of(String sourceName) {
        return new ModelSourceKey(sourceName);
    }

    /**
     * 优先使用表名生成来源键，表名缺失时再使用数据集标识。
     *
     * @param datasetId 逻辑数据集标识
     * @param tableName 来源表名
     * @return 模型来源键
     */
    public static ModelSourceKey fromDatasetAndTable(
            String datasetId,
            String tableName) {
        String preferred = tableName == null || tableName.trim().isEmpty()
                ? ValueUtils.requireNotBlank(datasetId, "模型数据集标识")
                : tableName;
        return of(preferred);
    }

    public String getSourceName() {
        return sourceName;
    }
}
