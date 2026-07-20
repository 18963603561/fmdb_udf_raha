package com.fiberhome.ml.raha.model.domain;

import com.fiberhome.ml.raha.data.loader.identity.RowIdentityConfig;
import com.fiberhome.ml.raha.data.type.ModelStatus;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 聚合一个不可变模型集合的公共版本、模式、行身份和列模型清单。
 */
public final class ModelSetManifest {

    /** 不可变模型集合版本。 */
    private final String modelSetVersion;
    /** 训练逻辑数据集标识。 */
    private final String datasetId;
    /** 训练输入模式哈希。 */
    private final String schemaHash;
    /** 模型集合共用的策略计划版本。 */
    private final String strategyPlanVersion;
    /** 模型集合共用的行身份规则。 */
    private final RowIdentityConfig rowIdentityConfig;
    /** 按字段名称稳定排序的列模型元数据。 */
    private final Map<String, RahaColumnModel> modelsByColumn;
    /** 全部列模型是否已经发布。 */
    private final boolean published;

    public ModelSetManifest(String modelSetVersion,
                            List<RahaColumnModel> models) {
        this.modelSetVersion = ValueUtils.requireNotBlank(
                modelSetVersion, "模型集合版本");
        if (models == null || models.isEmpty()) {
            throw new IllegalArgumentException("模型集合至少包含一个列模型");
        }
        for (RahaColumnModel model : models) {
            if (model == null) {
                throw new IllegalArgumentException("模型集合不能包含空列模型");
            }
        }
        List<RahaColumnModel> sorted = new ArrayList<RahaColumnModel>(models);
        Collections.sort(sorted, Comparator.comparing(
                RahaColumnModel::getColumnName));
        RahaColumnModel first = sorted.get(0);
        this.datasetId = first.getDatasetId();
        this.schemaHash = first.getSchemaHash();
        this.strategyPlanVersion = first.getStrategyPlanVersion();
        this.rowIdentityConfig = first.getRowIdentityConfig();
        Map<String, RahaColumnModel> byColumn =
                new LinkedHashMap<String, RahaColumnModel>();
        boolean allPublished = true;
        for (RahaColumnModel model : sorted) {
            if (model == null
                    || !this.modelSetVersion.equals(model.getModelSetVersion())
                    || !datasetId.equals(model.getDatasetId())
                    || !schemaHash.equals(model.getSchemaHash())
                    || !strategyPlanVersion.equals(model.getStrategyPlanVersion())
                    || !sameIdentity(rowIdentityConfig,
                    model.getRowIdentityConfig())) {
                throw new IllegalArgumentException("模型集合公共版本或兼容信息不一致");
            }
            if (byColumn.put(model.getColumnName(), model) != null) {
                throw new IllegalArgumentException("模型集合包含重复字段："
                        + model.getColumnName());
            }
            // 显式历史版本允许加载后来被停用的模型，但从未发布的候选或失败模型不能进入生产检测。
            allPublished &= model.getPublishedAt() != null
                    && (model.getStatus() == ModelStatus.PUBLISHED
                    || model.getStatus() == ModelStatus.DISABLED);
        }
        this.modelsByColumn = Collections.unmodifiableMap(byColumn);
        this.published = allPublished;
    }

    public String getModelSetVersion() { return modelSetVersion; }
    public String getDatasetId() { return datasetId; }
    public String getSchemaHash() { return schemaHash; }
    public String getStrategyPlanVersion() { return strategyPlanVersion; }
    public RowIdentityConfig getRowIdentityConfig() { return rowIdentityConfig; }
    public Map<String, RahaColumnModel> getModelsByColumn() {
        return modelsByColumn;
    }
    public boolean isPublished() { return published; }

    /**
     * 校验模型集合全部列模型已经发布。
     *
     * @return 当前模型集合，便于请求工厂链式使用
     */
    public ModelSetManifest requirePublished() {
        if (!published) {
            throw new IllegalStateException("模型集合尚未全部发布："
                    + modelSetVersion);
        }
        return this;
    }

    private static boolean sameIdentity(RowIdentityConfig first,
                                        RowIdentityConfig second) {
        return first != null && second != null
                && first.getMode() == second.getMode()
                && first.getKeyColumns().equals(second.getKeyColumns())
                && first.getFingerprintAlgorithm()
                == second.getFingerprintAlgorithm()
                && first.getNormalizationVersion().equals(
                second.getNormalizationVersion());
    }
}
