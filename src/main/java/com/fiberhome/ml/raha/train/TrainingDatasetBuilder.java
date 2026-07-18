package com.fiberhome.ml.raha.train;

import com.fiberhome.ml.raha.data.CellLabel;
import com.fiberhome.ml.raha.sample.SampleTuple;
import com.fiberhome.ml.raha.support.HashUtils;
import com.fiberhome.ml.raha.support.JsonUtils;
import com.fiberhome.ml.raha.support.RahaErrorCode;
import com.fiberhome.ml.raha.support.RahaException;
import com.fiberhome.ml.raha.support.ValueNormalizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 将采样行和直接标签构建为训练前原始样本，并执行同值传播。
 */
public final class TrainingDatasetBuilder {

    /**
     * 构建直接样本和同值一致传播样本。
     *
     * @param tuples 采样元组
     * @param labels 直接标签
     * @param targetColumns 训练目标字段
     * @return 原始训练样本
     */
    public List<RawTrainingExample> build(List<SampleTuple> tuples,
                                          List<CellLabel> labels,
                                          List<String> targetColumns) {
        Map<String, CellLabel> labelByCell = new HashMap<String, CellLabel>();
        for (CellLabel label : labels) {
            String key = key(label.getSampleBatchId(), label.getRowId(),
                    label.getColumnName());
            CellLabel previous = labelByCell.put(key, label);
            if (previous != null && previous.getLabel() != label.getLabel()) {
                throw new RahaException(RahaErrorCode.INVALID_DATA,
                        "同一单元格存在冲突直接标签：" + key);
            }
        }
        List<RawTrainingExample> direct = new ArrayList<RawTrainingExample>();
        Map<String, Integer> consensusByValue = new LinkedHashMap<String, Integer>();
        Map<String, Boolean> conflictByValue = new HashMap<String, Boolean>();
        for (SampleTuple tuple : tuples) {
            Map<String, String> row = JsonUtils.parseStringMap(tuple.getRowDataJson());
            for (String column : targetColumns) {
                CellLabel label = labelByCell.get(key(tuple.getSampleBatchId(),
                        tuple.getRowId(), column));
                if (label != null) {
                    String value = ValueNormalizer.normalize(row.get(column));
                    if (!HashUtils.sha256(value).equals(label.getValueHash())) {
                        throw new RahaException(RahaErrorCode.INVALID_DATA,
                                "标签原值已经变化，batchId=" + tuple.getSampleBatchId()
                                        + "，rowId=" + tuple.getRowId()
                                        + "，column=" + column);
                    }
                    direct.add(new RawTrainingExample(tuple.getSampleBatchId(),
                            tuple.getSnapshotId(), tuple.getRowId(), column, value,
                            tuple.getDuplicateCount(), label.getLabel(), "DIRECT",
                            tuple.getDuplicateCount()));
                    String valueKey = column + '|' + value;
                    Integer previous = consensusByValue.put(valueKey, label.getLabel());
                    if (previous != null && previous.intValue() != label.getLabel()) {
                        conflictByValue.put(valueKey, Boolean.TRUE);
                    }
                }
            }
        }
        List<RawTrainingExample> result = new ArrayList<RawTrainingExample>(direct);
        for (SampleTuple tuple : tuples) {
            Map<String, String> row = JsonUtils.parseStringMap(tuple.getRowDataJson());
            for (String column : targetColumns) {
                if (labelByCell.containsKey(key(tuple.getSampleBatchId(),
                        tuple.getRowId(), column))) {
                    continue;
                }
                String value = ValueNormalizer.normalize(row.get(column));
                String valueKey = column + '|' + value;
                Integer propagated = consensusByValue.get(valueKey);
                if (propagated != null && !conflictByValue.containsKey(valueKey)) {
                    result.add(new RawTrainingExample(null, tuple.getSnapshotId(),
                            tuple.getRowId(), column, value, tuple.getDuplicateCount(),
                            propagated, "PROPAGATED", tuple.getDuplicateCount() * 0.5d));
                }
            }
        }
        return result;
    }

    private static String key(String batchId, String rowId, String column) {
        return batchId + '|' + rowId + '|' + column;
    }
}
