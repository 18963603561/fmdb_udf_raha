package com.fiberhome.ml.raha.evaluation;

import com.fiberhome.ml.raha.model.RahaColumnModel;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.ModelMetadataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 比较候选阈值，按确定性指标规则选优并写回模型元数据。
 */
public final class ThresholdComparisonService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ThresholdComparisonService.class);
    /** 检测指标计算器。 */
    private final DetectionEvaluationService evaluationService;
    /** 模型元数据仓储。 */
    private final ModelMetadataRepository repository;
    /** 提供可测试更新时间的时钟。 */
    private final Clock clock;

    public ThresholdComparisonService(DetectionEvaluationService evaluationService,
                                      ModelMetadataRepository repository,
                                      Clock clock) {
        if (evaluationService == null || repository == null || clock == null) {
            throw new IllegalArgumentException("阈值比较服务依赖不能为空");
        }
        this.evaluationService = evaluationService;
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 评测候选阈值，按 F1、精确率、召回率和较低阈值顺序选优并保存。
     *
     * @param model 待评测草稿或候选模型
     * @param scores 当前模型对评测单元格的分数
     * @param groundTruth 全量单元格真值标签
     * @param candidateThresholds 候选阈值集合
     * @param version 模型元数据仓储业务版本
     * @return 阈值评测和已更新模型元数据
     */
    public ThresholdComparisonResult compareAndSave(
            RahaColumnModel model,
            List<CellScore> scores,
            List<com.fiberhome.ml.raha.label.CellLabel> groundTruth,
            List<Double> candidateThresholds,
            ArtifactVersion version) {
        if (model == null || scores == null || groundTruth == null
                || candidateThresholds == null || candidateThresholds.isEmpty()
                || version == null) {
            throw new IllegalArgumentException("阈值比较输入和版本不能为空");
        }
        List<Double> thresholds = thresholds(candidateThresholds);
        LOGGER.info("开始模型阈值比较，datasetId={}，columnName={}，modelVersion={}，"
                        + "candidateCount={}",
                model.getDatasetId(), model.getColumnName(), model.getModelVersion(),
                thresholds.size());
        List<ThresholdEvaluation> evaluations = new ArrayList<ThresholdEvaluation>();
        for (Double threshold : thresholds) {
            evaluations.add(new ThresholdEvaluation(threshold,
                    evaluationService.evaluateAtThreshold(
                            scores, groundTruth, threshold)));
        }
        ThresholdEvaluation selected = evaluations.get(0);
        for (ThresholdEvaluation candidate : evaluations) {
            if (better(candidate, selected)) {
                selected = candidate;
            }
        }
        Map<String, Double> metrics = evaluationMetrics(selected.getMetrics());
        RahaColumnModel updated = model.withEvaluation(
                selected.getThreshold(), metrics);
        repository.saveAll(Collections.singletonList(updated), version, clock.millis());
        LOGGER.info("模型阈值比较完成，datasetId={}，columnName={}，modelVersion={}，"
                        + "selectedThreshold={}，f1={}",
                model.getDatasetId(), model.getColumnName(), model.getModelVersion(),
                selected.getThreshold(), selected.getMetrics().getF1());
        return new ThresholdComparisonResult(evaluations,
                selected.getThreshold(), updated);
    }

    /**
     * 在召回下限约束内优先提高精确率并降低假阳性，适用于误报集中的字段。
     *
     * @param model 待评测候选模型
     * @param scores 独立验证集分数
     * @param groundTruth 独立验证集真值
     * @param candidateThresholds 候选阈值
     * @param policy 阈值选择约束
     * @param version 模型元数据仓储业务版本
     * @return 阈值评测和已更新模型元数据
     */
    public ThresholdComparisonResult compareAndSave(
            RahaColumnModel model,
            List<CellScore> scores,
            List<com.fiberhome.ml.raha.label.CellLabel> groundTruth,
            List<Double> candidateThresholds,
            ThresholdSelectionPolicy policy,
            ArtifactVersion version) {
        if (model == null || scores == null || groundTruth == null
                || candidateThresholds == null || candidateThresholds.isEmpty()
                || policy == null || version == null) {
            throw new IllegalArgumentException("约束阈值比较输入、策略和版本不能为空");
        }
        List<Double> thresholds = thresholds(candidateThresholds);
        List<ThresholdEvaluation> evaluations = new ArrayList<ThresholdEvaluation>();
        for (Double threshold : thresholds) {
            evaluations.add(new ThresholdEvaluation(threshold,
                    evaluationService.evaluateAtThreshold(
                            scores, groundTruth, threshold)));
        }
        DetectionEvaluationMetrics baseline = evaluationService.evaluateAtThreshold(
                scores, groundTruth, policy.getBaselineThreshold());
        double recallFloor = Math.max(policy.getMinimumRecall(),
                baseline.getRecall() - policy.getMaximumRecallDrop());
        ThresholdEvaluation selected = null;
        for (ThresholdEvaluation candidate : evaluations) {
            if (candidate.getMetrics().getRecall() + 1.0e-12d < recallFloor) {
                continue;
            }
            if (selected == null || precisionFirst(candidate, selected)) {
                selected = candidate;
            }
        }
        // 极端小验证集无法满足召回约束时退回原 F1 规则，但保留回退指标用于审计。
        boolean constraintFallback = selected == null;
        if (selected == null) {
            selected = evaluations.get(0);
            for (ThresholdEvaluation candidate : evaluations) {
                if (better(candidate, selected)) {
                    selected = candidate;
                }
            }
        }
        Map<String, Double> metrics = evaluationMetrics(selected.getMetrics());
        metrics.put("evaluation.recallFloor", recallFloor);
        metrics.put("evaluation.baselineRecall", baseline.getRecall());
        metrics.put("evaluation.baselineThreshold", policy.getBaselineThreshold());
        metrics.put("evaluation.constraintFallback", constraintFallback ? 1.0d : 0.0d);
        RahaColumnModel updated = model.withEvaluation(
                selected.getThreshold(), metrics);
        repository.saveAll(Collections.singletonList(updated), version, clock.millis());
        LOGGER.info("约束阈值比较完成，datasetId={}，columnName={}，modelVersion={}，"
                        + "selectedThreshold={}，precision={}，recall={}，recallFloor={}，"
                        + "constraintFallback={}",
                model.getDatasetId(), model.getColumnName(), model.getModelVersion(),
                selected.getThreshold(), selected.getMetrics().getPrecision(),
                selected.getMetrics().getRecall(), recallFloor, constraintFallback);
        return new ThresholdComparisonResult(evaluations,
                selected.getThreshold(), updated);
    }

    private static List<Double> thresholds(List<Double> values) {
        Set<Double> unique = new LinkedHashSet<Double>();
        for (Double value : values) {
            if (value == null || Double.isNaN(value) || value < 0.0d || value > 1.0d) {
                throw new IllegalArgumentException("候选阈值必须位于零到一之间");
            }
            unique.add(value);
        }
        List<Double> sorted = new ArrayList<Double>(unique);
        Collections.sort(sorted);
        return sorted;
    }

    private static boolean better(ThresholdEvaluation candidate,
                                  ThresholdEvaluation selected) {
        DetectionEvaluationMetrics candidateMetrics = candidate.getMetrics();
        DetectionEvaluationMetrics selectedMetrics = selected.getMetrics();
        int f1 = Double.compare(candidateMetrics.getF1(), selectedMetrics.getF1());
        if (f1 != 0) {
            return f1 > 0;
        }
        int precision = Double.compare(
                candidateMetrics.getPrecision(), selectedMetrics.getPrecision());
        if (precision != 0) {
            return precision > 0;
        }
        int recall = Double.compare(
                candidateMetrics.getRecall(), selectedMetrics.getRecall());
        return recall != 0 ? recall > 0
                : candidate.getThreshold() < selected.getThreshold();
    }

    private static boolean precisionFirst(ThresholdEvaluation candidate,
                                          ThresholdEvaluation selected) {
        DetectionEvaluationMetrics candidateMetrics = candidate.getMetrics();
        DetectionEvaluationMetrics selectedMetrics = selected.getMetrics();
        int precision = Double.compare(
                candidateMetrics.getPrecision(), selectedMetrics.getPrecision());
        if (precision != 0) {
            return precision > 0;
        }
        int falsePositive = Long.compare(
                candidateMetrics.getFalsePositive(), selectedMetrics.getFalsePositive());
        if (falsePositive != 0) {
            return falsePositive < 0;
        }
        int f1 = Double.compare(candidateMetrics.getF1(), selectedMetrics.getF1());
        if (f1 != 0) {
            return f1 > 0;
        }
        int recall = Double.compare(
                candidateMetrics.getRecall(), selectedMetrics.getRecall());
        return recall != 0 ? recall > 0
                : candidate.getThreshold() > selected.getThreshold();
    }

    private static Map<String, Double> evaluationMetrics(
            DetectionEvaluationMetrics metrics) {
        Map<String, Double> values = new LinkedHashMap<String, Double>();
        values.put("evaluation.precision", metrics.getPrecision());
        values.put("evaluation.recall", metrics.getRecall());
        values.put("evaluation.f1", metrics.getF1());
        values.put("evaluation.averagePrecision", metrics.getAveragePrecision());
        values.put("evaluation.threshold", metrics.getThreshold());
        values.put("evaluation.cellCount", (double) metrics.getEvaluatedCellCount());
        return values;
    }
}
