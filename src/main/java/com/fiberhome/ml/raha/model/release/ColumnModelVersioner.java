package com.fiberhome.ml.raha.model.release;

import com.fiberhome.ml.raha.data.type.ClassifierType;
import com.fiberhome.ml.raha.model.training.ColumnModelTrainingRequest;
import java.util.Map;

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
        if (request.getModelSetVersion() != null) {
            return ModelReadableVersioner.columnModelVersion(
                    request.getModelSetVersion(),
                    request.getDataset().getColumnName());
        }
        String source = request.getModelSourceName() == null
                ? request.getDatasetId() : request.getModelSourceName();
        return ModelReadableVersioner.columnModelVersion(source,
                request.getDataset().getColumnName(),
                request.getStrategyPlanVersion());
    }
}
