package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.label.CellLabel;
import com.fiberhome.ml.raha.util.HashUtils;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 使用稳定单元格哈希划分阈值验证集和最终测试集。
 */
public final class EvaluationSplitService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            EvaluationSplitService.class);

    /**
     * 排除直接标注坐标后执行确定性划分。
     *
     * @param labels 全量评测真值
     * @param directCellIds 训练直接标签坐标
     * @param validationModulo 验证集哈希模数
     * @param validationBucket 验证集命中的桶编号
     * @param salt 划分版本盐值
     * @return 无重叠的阈值验证集和最终测试集
     */
    public EvaluationSplit split(List<CellLabel> labels,
                                 Set<String> directCellIds,
                                 int validationModulo,
                                 int validationBucket,
                                 String salt) {
        if (labels == null || directCellIds == null || validationModulo <= 1
                || validationBucket < 0 || validationBucket >= validationModulo) {
            throw new IllegalArgumentException("评测划分输入和桶配置必须有效");
        }
        String validatedSalt = ValueUtils.requireNotBlank(salt, "评测划分盐值");
        List<CellLabel> validation = new ArrayList<CellLabel>();
        List<CellLabel> test = new ArrayList<CellLabel>();
        for (CellLabel label : labels) {
            if (label == null) {
                throw new IllegalArgumentException("评测真值不能包含空标签");
            }
            if (directCellIds.contains(label.getCellId())) {
                continue;
            }
            String hash = HashUtils.sha256Hex(validatedSalt + "|" + label.getCellId());
            long bucketValue = Long.parseLong(hash.substring(0, 8), 16);
            if (bucketValue % validationModulo == validationBucket) {
                validation.add(label);
            } else {
                test.add(label);
            }
        }
        Collections.sort(validation, Comparator.comparing(CellLabel::getCellId));
        Collections.sort(test, Comparator.comparing(CellLabel::getCellId));
        int eligibleCount = validation.size() + test.size();
        if (eligibleCount < 2) {
            throw new IllegalStateException("评测划分至少需要两个非训练坐标");
        }
        // 极小数据集可能全部落入同一哈希桶，稳定移动一个坐标以保持两侧非空。
        if (validation.isEmpty()) {
            validation.add(test.remove(0));
            LOGGER.warn("评测验证集哈希结果为空，已按稳定坐标顺序重平衡，"
                    + "eligibleCount={}", eligibleCount);
        } else if (test.isEmpty()) {
            test.add(validation.remove(validation.size() - 1));
            LOGGER.warn("最终测试集哈希结果为空，已按稳定坐标顺序重平衡，"
                    + "eligibleCount={}", eligibleCount);
        }
        return new EvaluationSplit(validation, test);
    }
}
