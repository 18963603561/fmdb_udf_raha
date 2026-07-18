package com.fiberhome.ml.raha.model.training;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 保存单列训练样本、类别统计、特征维度和不可训练原因。
 */
public final class ColumnTrainingDataset {

    /** 目标字段。 */
    private final String columnName;
    /** 特征字典版本。 */
    private final String featureDictionaryVersion;
    /** 特征维度。 */
    private final int featureDimension;
    /** 训练样本。 */
    private final List<ColumnTrainingExample> examples;
    /** 错误标签数量。 */
    private final int positiveCount;
    /** 正常标签数量。 */
    private final int negativeCount;
    /** 因直接标签冲突被剔除的单元格数量。 */
    private final int conflictingCellCount;
    /** 数据可用性状态。 */
    private final ColumnTrainingStatus status;
    /** 不包含原始值的状态说明。 */
    private final String message;

    public ColumnTrainingDataset(String columnName,
                                 String featureDictionaryVersion,
                                 int featureDimension,
                                 List<ColumnTrainingExample> examples,
                                 int positiveCount,
                                 int negativeCount,
                                 int conflictingCellCount,
                                 ColumnTrainingStatus status,
                                 String message) {
        this.columnName = ValueUtils.requireNotBlank(columnName, "训练字段");
        this.featureDictionaryVersion = ValueUtils.requireNotBlank(
                featureDictionaryVersion, "特征字典版本");
        if (featureDimension < 0 || examples == null || positiveCount < 0
                || negativeCount < 0 || conflictingCellCount < 0 || status == null
                || positiveCount + negativeCount != examples.size()) {
            throw new IllegalArgumentException("列级训练数据统计非法");
        }
        this.featureDimension = featureDimension;
        this.examples = Collections.unmodifiableList(
                new ArrayList<ColumnTrainingExample>(examples));
        this.positiveCount = positiveCount;
        this.negativeCount = negativeCount;
        this.conflictingCellCount = conflictingCellCount;
        this.status = status;
        this.message = message;
    }

    public String getColumnName() { return columnName; }
    public String getFeatureDictionaryVersion() { return featureDictionaryVersion; }
    public int getFeatureDimension() { return featureDimension; }
    public List<ColumnTrainingExample> getExamples() { return examples; }
    public int getPositiveCount() { return positiveCount; }
    public int getNegativeCount() { return negativeCount; }
    public int getConflictingCellCount() { return conflictingCellCount; }
    public ColumnTrainingStatus getStatus() { return status; }
    public String getMessage() { return message; }
}
