package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.util.HashUtils;

import java.util.Map;
import java.util.TreeMap;

/**
 * 根据训练依赖和最终线性参数生成稳定列级模型版本。
 */
public final class ColumnModelVersioner {

    public String versionOf(ColumnModelTrainingRequest request,
                            ClassifierType classifierType,
                            double intercept,
                            Map<Integer, Double> coefficients,
                            String trainingMode) {
        if (request == null || classifierType == null
                || coefficients == null || trainingMode == null) {
            throw new IllegalArgumentException("模型版本参数不能为空");
        }
        StringBuilder canonical = new StringBuilder();
        canonical.append(request.getModelName()).append('|')
                .append(request.getDatasetId()).append('|')
                .append(request.getSchemaHash()).append('|')
                .append(request.getStrategyPlanVersion()).append('|')
                .append(request.getDataset().getColumnName()).append('|')
                .append(request.getDataset().getFeatureDictionaryVersion()).append('|')
                .append(request.getModelConfig().getThreshold()).append('|')
                .append(classifierType.name()).append('|').append(trainingMode).append('|')
                .append(intercept);
        for (Map.Entry<Integer, Double> entry
                : new TreeMap<Integer, Double>(coefficients).entrySet()) {
            canonical.append('|').append(entry.getKey()).append('=').append(entry.getValue());
        }
        return HashUtils.sha256Hex(canonical.toString());
    }

    /**
     * 根据树模型编码生成稳定版本，编码摘要用于隔离不同树结构。
     */
    public String versionOfTree(ColumnModelTrainingRequest request,
                                ClassifierType classifierType,
                                String trainingMode,
                                String modelPayload) {
        if (request == null || classifierType == null || trainingMode == null
                || modelPayload == null) {
            throw new IllegalArgumentException("树模型版本参数不能为空");
        }
        String canonical = request.getModelName() + '|'
                + request.getDatasetId() + '|'
                + request.getSchemaHash() + '|'
                + request.getStrategyPlanVersion() + '|'
                + request.getDataset().getColumnName() + '|'
                + request.getDataset().getFeatureDictionaryVersion() + '|'
                + request.getModelConfig().getThreshold() + '|'
                + classifierType.name() + '|' + trainingMode + '|'
                + HashUtils.sha256Hex(modelPayload);
        return HashUtils.sha256Hex(canonical);
    }
}
