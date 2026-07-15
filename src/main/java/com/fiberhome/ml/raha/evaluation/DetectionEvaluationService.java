package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.label.CellLabel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 计算单元格级混淆矩阵、精确率、召回率、F1 和平均精确率。
 */
public final class DetectionEvaluationService {

    /**
     * 使用检测结果自身的错误判断计算指标，缺少检测结果的真值单元格按未检出处理。
     *
     * @param detections 单元格检测结果
     * @param groundTruth 全量单元格真值标签
     * @return 检测评测指标
     */
    public DetectionEvaluationMetrics evaluate(List<DetectionResult> detections,
                                                List<CellLabel> groundTruth) {
        if (detections == null) {
            throw new IllegalArgumentException("检测结果不能为空");
        }
        Map<String, ScoredPrediction> predictions =
                new LinkedHashMap<String, ScoredPrediction>();
        for (DetectionResult detection : detections) {
            if (detection == null || predictions.put(detection.getCoordinate().toCellId(),
                    new ScoredPrediction(detection.getScore(), detection.isError())) != null) {
                throw new IllegalArgumentException("检测结果不能包含空值或重复单元格");
            }
        }
        return calculate(predictions, truth(groundTruth), null);
    }

    /**
     * 使用统一候选阈值评测单元格分数，供模型阈值选择使用。
     *
     * @param scores 单元格错误分数
     * @param groundTruth 全量单元格真值标签
     * @param threshold 候选判断阈值
     * @return 当前阈值评测指标
     */
    public DetectionEvaluationMetrics evaluateAtThreshold(List<CellScore> scores,
                                                           List<CellLabel> groundTruth,
                                                           double threshold) {
        if (scores == null || Double.isNaN(threshold)
                || threshold < 0.0d || threshold > 1.0d) {
            throw new IllegalArgumentException("评测分数和阈值必须有效");
        }
        Map<String, ScoredPrediction> predictions =
                new LinkedHashMap<String, ScoredPrediction>();
        for (CellScore score : scores) {
            if (score == null || predictions.put(score.getCellId(),
                    new ScoredPrediction(score.getScore(),
                            score.getScore() >= threshold)) != null) {
                throw new IllegalArgumentException("评测分数不能包含空值或重复单元格");
            }
        }
        return calculate(predictions, truth(groundTruth), threshold);
    }

    private static DetectionEvaluationMetrics calculate(
            Map<String, ScoredPrediction> predictions,
            Map<String, Integer> truth,
            Double threshold) {
        for (String cellId : predictions.keySet()) {
            if (!truth.containsKey(cellId)) {
                throw new IllegalArgumentException("检测分数缺少对应真值标签：" + cellId);
            }
        }
        long truePositive = 0L;
        long falsePositive = 0L;
        long falseNegative = 0L;
        long trueNegative = 0L;
        List<RankedTruth> ranked = new ArrayList<RankedTruth>(truth.size());
        for (Map.Entry<String, Integer> entry : truth.entrySet()) {
            ScoredPrediction prediction = predictions.get(entry.getKey());
            boolean predicted = prediction != null && prediction.predicted;
            double score = prediction == null ? 0.0d : prediction.score;
            if (entry.getValue() == 1 && predicted) {
                truePositive++;
            } else if (entry.getValue() == 0 && predicted) {
                falsePositive++;
            } else if (entry.getValue() == 1) {
                falseNegative++;
            } else {
                trueNegative++;
            }
            ranked.add(new RankedTruth(entry.getKey(), score, entry.getValue()));
        }
        double precision = ratio(truePositive, truePositive + falsePositive);
        double recall = ratio(truePositive, truePositive + falseNegative);
        double f1 = precision + recall == 0.0d
                ? 0.0d : 2.0d * precision * recall / (precision + recall);
        double averagePrecision = averagePrecision(ranked,
                truePositive + falseNegative);
        return new DetectionEvaluationMetrics(truth.size(), predictions.size(),
                truePositive, falsePositive, falseNegative, trueNegative,
                precision, recall, f1, averagePrecision, threshold);
    }

    private static Map<String, Integer> truth(List<CellLabel> labels) {
        if (labels == null) {
            throw new IllegalArgumentException("真值标签不能为空");
        }
        Map<String, Integer> truth = new LinkedHashMap<String, Integer>();
        for (CellLabel label : labels) {
            if (label == null || label.getLabelSource() != LabelSource.GROUND_TRUTH
                    || truth.put(label.getCellId(), label.getLabel()) != null) {
                throw new IllegalArgumentException("评测只接受唯一的直接真值标签");
            }
        }
        return truth;
    }

    private static double averagePrecision(List<RankedTruth> ranked,
                                           long positiveCount) {
        if (positiveCount == 0L) {
            return 0.0d;
        }
        Collections.sort(ranked, Comparator
                .comparingDouble(RankedTruth::getScore).reversed()
                .thenComparing(RankedTruth::getCellId));
        long cumulativePositive = 0L;
        long cumulativeCount = 0L;
        double averagePrecision = 0.0d;
        int index = 0;
        while (index < ranked.size()) {
            double score = ranked.get(index).score;
            long groupPositive = 0L;
            long groupCount = 0L;
            while (index < ranked.size()
                    && Double.compare(ranked.get(index).score, score) == 0) {
                groupPositive += ranked.get(index).label;
                groupCount++;
                index++;
            }
            cumulativePositive += groupPositive;
            cumulativeCount += groupCount;
            if (groupPositive > 0L) {
                double recallIncrement = (double) groupPositive / positiveCount;
                double precision = (double) cumulativePositive / cumulativeCount;
                averagePrecision += recallIncrement * precision;
            }
        }
        return averagePrecision;
    }

    private static double ratio(long numerator, long denominator) {
        return denominator == 0L ? 0.0d : (double) numerator / denominator;
    }

    private static final class ScoredPrediction {
        /** 单元格错误分数。 */
        private final double score;
        /** 当前阈值下是否预测为错误。 */
        private final boolean predicted;

        private ScoredPrediction(double score, boolean predicted) {
            this.score = score;
            this.predicted = predicted;
        }
    }

    private static final class RankedTruth {
        /** 稳定单元格标识。 */
        private final String cellId;
        /** 排序使用的错误分数。 */
        private final double score;
        /** 零一真值标签。 */
        private final int label;

        private RankedTruth(String cellId, double score, int label) {
            this.cellId = cellId;
            this.score = score;
            this.label = label;
        }

        private String getCellId() { return cellId; }
        private double getScore() { return score; }
    }
}
