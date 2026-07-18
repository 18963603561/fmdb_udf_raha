package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.label.CellLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按字段关联特征和标签，直接标签优先，并计算类别平衡权重。
 */
public final class ColumnTrainingDataBuilder {

    /**
     * 构建单列训练集，冲突标签单元格会被剔除而不是静默选择。
     *
     * @param columnName 目标字段
     * @param dictionary 特征字典
     * @param rows 单列稀疏特征
     * @param labels 直接和传播标签
     * @param classBalanceEnabled 是否启用类别平衡权重
     * @return 列级训练数据和可用性状态
     */
    public ColumnTrainingDataset build(String columnName,
                                       FeatureDictionary dictionary,
                                       List<SparseFeatureRow> rows,
                                       List<CellLabel> labels,
                                       boolean classBalanceEnabled) {
        if (columnName == null || columnName.trim().isEmpty()
                || dictionary == null || rows == null || labels == null
                || !columnName.equals(dictionary.getColumnName())) {
            throw new IllegalArgumentException("列级训练数据参数不能为空且字段必须一致");
        }
        if (dictionary.getDefinitions().isEmpty()) {
            return result(columnName, dictionary, Collections.<ColumnTrainingExample>emptyList(),
                    0, 0, 0, ColumnTrainingStatus.EMPTY_FEATURES,
                    "当前字段没有有效特征");
        }
        Map<String, List<CellLabel>> labelsByCell = labelsByCell(labels);
        List<SelectedExample> selected = new ArrayList<SelectedExample>();
        int conflictCount = 0;
        List<SparseFeatureRow> sortedRows = new ArrayList<SparseFeatureRow>(rows);
        Collections.sort(sortedRows, Comparator.comparing(SparseFeatureRow::getCellId));
        for (SparseFeatureRow row : sortedRows) {
            if (!columnName.equals(row.getColumnName())
                    || !dictionary.getVersion().equals(row.getFeatureDictionaryVersion())) {
                throw new IllegalArgumentException("训练特征行与字段或字典版本不一致");
            }
            LabelSelection selection = selectLabel(labelsByCell.get(row.getCellId()));
            if (selection.conflict) {
                conflictCount++;
                continue;
            }
            if (selection.label != null) {
                selected.add(new SelectedExample(row, selection.label));
            }
        }
        if (selected.isEmpty()) {
            ColumnTrainingStatus status = conflictCount > 0
                    ? ColumnTrainingStatus.LABEL_CONFLICT : ColumnTrainingStatus.NO_LABELS;
            return result(columnName, dictionary, Collections.<ColumnTrainingExample>emptyList(),
                    0, 0, conflictCount, status,
                    status == ColumnTrainingStatus.LABEL_CONFLICT
                            ? "标签冲突剔除后没有训练样本" : "当前字段没有可关联标签");
        }
        int positives = 0;
        for (SelectedExample example : selected) {
            positives += example.label.getLabel();
        }
        int negatives = selected.size() - positives;
        List<ColumnTrainingExample> examples = new ArrayList<ColumnTrainingExample>();
        for (SelectedExample example : selected) {
            double classWeight = classBalanceEnabled
                    ? classWeight(example.label.getLabel(), selected.size(), positives, negatives)
                    : 1.0d;
            examples.add(new ColumnTrainingExample(example.row.getCellId(),
                    example.label.getLabel(), example.label.getLabelSource(),
                    example.row.getValues(), example.label.getSampleWeight() * classWeight));
        }
        ColumnTrainingStatus status = positives == 0 || negatives == 0
                ? ColumnTrainingStatus.SINGLE_CLASS : ColumnTrainingStatus.TRAINABLE;
        return result(columnName, dictionary, examples, positives, negatives,
                conflictCount, status, status == ColumnTrainingStatus.TRAINABLE
                        ? "训练数据已就绪" : "当前字段只有一个标签类别");
    }

    private static ColumnTrainingDataset result(String columnName,
                                                FeatureDictionary dictionary,
                                                List<ColumnTrainingExample> examples,
                                                int positives,
                                                int negatives,
                                                int conflicts,
                                                ColumnTrainingStatus status,
                                                String message) {
        return new ColumnTrainingDataset(columnName, dictionary.getVersion(),
                dictionary.getDefinitions().size(), examples, positives, negatives,
                conflicts, status, message);
    }

    private static Map<String, List<CellLabel>> labelsByCell(List<CellLabel> labels) {
        Map<String, List<CellLabel>> index = new HashMap<String, List<CellLabel>>();
        for (CellLabel label : labels) {
            if (label == null) {
                throw new IllegalArgumentException("训练标签不能包含空值");
            }
            if (!index.containsKey(label.getCellId())) {
                index.put(label.getCellId(), new ArrayList<CellLabel>());
            }
            index.get(label.getCellId()).add(label);
        }
        return index;
    }

    private static LabelSelection selectLabel(List<CellLabel> labels) {
        if (labels == null || labels.isEmpty()) {
            return LabelSelection.none();
        }
        List<CellLabel> direct = new ArrayList<CellLabel>();
        List<CellLabel> propagated = new ArrayList<CellLabel>();
        for (CellLabel label : labels) {
            if (label.getLabelSource() == LabelSource.PROPAGATED) {
                propagated.add(label);
            } else {
                direct.add(label);
            }
        }
        List<CellLabel> candidates = direct.isEmpty() ? propagated : direct;
        int expected = candidates.get(0).getLabel();
        for (CellLabel candidate : candidates) {
            if (candidate.getLabel() != expected) {
                return LabelSelection.conflict();
            }
        }
        Collections.sort(candidates, new Comparator<CellLabel>() {
            @Override
            public int compare(CellLabel first, CellLabel second) {
                int weightCompare = Double.compare(
                        second.getSampleWeight(), first.getSampleWeight());
                if (weightCompare != 0) {
                    return weightCompare;
                }
                int timeCompare = Long.compare(second.getCreatedAt(), first.getCreatedAt());
                return timeCompare == 0
                        ? first.getLabelId().compareTo(second.getLabelId()) : timeCompare;
            }
        });
        return LabelSelection.selected(candidates.get(0));
    }

    private static double classWeight(int label,
                                      int total,
                                      int positives,
                                      int negatives) {
        int categoryCount = label == 1 ? positives : negatives;
        return categoryCount == 0 ? 1.0d : (double) total / (2.0d * categoryCount);
    }

    private static final class SelectedExample {
        /** 单元格特征行。 */
        private final SparseFeatureRow row;
        /** 直接优先解析后的标签。 */
        private final CellLabel label;

        private SelectedExample(SparseFeatureRow row, CellLabel label) {
            this.row = row;
            this.label = label;
        }
    }

    private static final class LabelSelection {
        /** 选中的标签，没有标签或冲突时为空。 */
        private final CellLabel label;
        /** 当前单元格候选标签是否冲突。 */
        private final boolean conflict;

        private LabelSelection(CellLabel label, boolean conflict) {
            this.label = label;
            this.conflict = conflict;
        }

        private static LabelSelection none() { return new LabelSelection(null, false); }
        private static LabelSelection conflict() { return new LabelSelection(null, true); }
        private static LabelSelection selected(CellLabel label) {
            return new LabelSelection(label, false);
        }
    }
}
