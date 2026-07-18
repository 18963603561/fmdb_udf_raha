package com.fiberhome.ml.raha.model.prediction;

import com.fiberhome.ml.raha.feature.domain.SparseFeatureRow;
import com.fiberhome.ml.raha.model.domain.ColumnModelArtifact;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 使用已加载列级模型参数批量输出单元格分数和阈值判断。
 */
public final class ColumnModelPredictor {

    public List<ColumnPrediction> predict(ColumnModelArtifact model,
                                          List<SparseFeatureRow> rows) {
        if (model == null || rows == null) {
            throw new IllegalArgumentException("列级预测模型和特征不能为空");
        }
        List<ColumnPrediction> predictions = new ArrayList<ColumnPrediction>(rows.size());
        for (SparseFeatureRow row : rows) {
            double score = model.score(row);
            predictions.add(new ColumnPrediction(row.getCellId(), score,
                    model.getThreshold(), score >= model.getThreshold(),
                    model.getClassifierType(), model.getModelVersion()));
        }
        return Collections.unmodifiableList(predictions);
    }
}
