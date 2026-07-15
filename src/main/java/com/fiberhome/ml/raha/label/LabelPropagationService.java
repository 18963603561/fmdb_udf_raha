package com.fiberhome.ml.raha.label;

import com.fiberhome.ml.raha.cluster.ClusterAssignment;
import com.fiberhome.ml.raha.data.LabelSource;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.CellLabelRepository;
import com.fiberhome.ml.raha.util.HashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * 在同一字段和聚类版本内传播直接标签，并保存冲突摘要。
 */
public final class LabelPropagationService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(LabelPropagationService.class);
    /** 标签和传播摘要仓储。 */
    private final CellLabelRepository repository;
    /** 提供可测试传播时间的时钟。 */
    private final Clock clock;

    public LabelPropagationService(CellLabelRepository repository, Clock clock) {
        if (repository == null || clock == null) {
            throw new IllegalArgumentException("标签传播服务依赖不能为空");
        }
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * 执行同质性或多数传播，直接标签始终原样保留且不会被传播标签覆盖。
     *
     * @param jobId 任务标识
     * @param assignments 聚类成员
     * @param directLabels 直接标签
     * @param method 传播方式
     * @param config 传播配置
     * @param version 仓储业务版本
     * @return 扩展标签、传播摘要和指标
     */
    public LabelPropagationResult propagateAndSave(String jobId,
                                                   List<ClusterAssignment> assignments,
                                                   List<CellLabel> directLabels,
                                                   LabelPropagationMethod method,
                                                   LabelPropagationConfig config,
                                                   ArtifactVersion version) {
        if (assignments == null || directLabels == null || method == null
                || config == null || version == null) {
            throw new IllegalArgumentException("标签传播输入、配置和版本不能为空");
        }
        for (CellLabel label : directLabels) {
            if (label == null || label.getLabelSource() == LabelSource.PROPAGATED) {
                throw new IllegalArgumentException("标签传播输入只能包含直接标签");
            }
        }
        Map<String, List<ClusterAssignment>> clusters = groupAssignments(assignments);
        Map<String, List<CellLabel>> labelsByCell = labelsByCell(directLabels);
        List<CellLabel> allLabels = new ArrayList<CellLabel>(directLabels);
        List<ClusterPropagationSummary> summaries =
                new ArrayList<ClusterPropagationSummary>();
        long propagatedCount = 0L;
        long conflictClusterCount = 0L;
        long unlabeledClusterCount = 0L;
        long createdAt = clock.millis();
        LOGGER.info("开始标签传播，jobId={}，clusterCount={}，directLabelCount={}，method={}",
                jobId, clusters.size(), directLabels.size(), method);
        for (List<ClusterAssignment> members : clusters.values()) {
            PropagationDecision decision = decide(
                    members, labelsByCell, method, config);
            List<CellLabel> propagated = decision.canPropagate
                    ? propagatedLabels(members, labelsByCell, decision, method,
                    config, createdAt) : Collections.<CellLabel>emptyList();
            allLabels.addAll(propagated);
            propagatedCount += propagated.size();
            if (decision.status == ClusterPropagationStatus.CONFLICT
                    || decision.status == ClusterPropagationStatus.NO_MAJORITY) {
                conflictClusterCount++;
            }
            if (decision.status == ClusterPropagationStatus.NO_LABELS) {
                unlabeledClusterCount++;
            }
            ClusterAssignment first = members.get(0);
            summaries.add(new ClusterPropagationSummary(first.getColumnName(),
                    first.getClusterId(), first.getClusterVersion(), method,
                    propagated.isEmpty() && decision.canPropagate
                            ? ClusterPropagationStatus.NO_UNLABELED_MEMBERS : decision.status,
                    decision.directCount, decision.errorCount, decision.normalCount,
                    decision.conflictCount, decision.majorityRatio, propagated.size()));
        }
        LabelPropagationMetrics metrics = new LabelPropagationMetrics(
                directLabels.size(), propagatedCount, conflictClusterCount,
                unlabeledClusterCount);
        repository.savePropagationResult(jobId, allLabels, summaries, version, createdAt);
        LOGGER.info("标签传播完成，jobId={}，directLabelCount={}，propagatedLabelCount={}，"
                        + "conflictClusterCount={}，unlabeledClusterCount={}",
                jobId, metrics.getDirectLabelCount(), metrics.getPropagatedLabelCount(),
                metrics.getConflictClusterCount(), metrics.getUnlabeledClusterCount());
        return new LabelPropagationResult(allLabels, summaries, metrics);
    }

    private static Map<String, List<ClusterAssignment>> groupAssignments(
            List<ClusterAssignment> assignments) {
        Map<String, List<ClusterAssignment>> clusters =
                new LinkedHashMap<String, List<ClusterAssignment>>();
        List<ClusterAssignment> sorted = new ArrayList<ClusterAssignment>(assignments);
        Collections.sort(sorted, Comparator.comparing(assignment -> clusterKey(assignment)
                + "|" + assignment.getCellId()));
        for (ClusterAssignment assignment : sorted) {
            if (assignment == null || assignment.getCoordinate() == null) {
                throw new IllegalArgumentException("标签传播要求聚类成员包含单元格坐标");
            }
            String key = clusterKey(assignment);
            if (!clusters.containsKey(key)) {
                clusters.put(key, new ArrayList<ClusterAssignment>());
            }
            clusters.get(key).add(assignment);
        }
        return clusters;
    }

    private static Map<String, List<CellLabel>> labelsByCell(List<CellLabel> labels) {
        Map<String, List<CellLabel>> index = new HashMap<String, List<CellLabel>>();
        for (CellLabel label : labels) {
            if (!index.containsKey(label.getCellId())) {
                index.put(label.getCellId(), new ArrayList<CellLabel>());
            }
            index.get(label.getCellId()).add(label);
        }
        return index;
    }

    private static PropagationDecision decide(List<ClusterAssignment> members,
                                              Map<String, List<CellLabel>> labelsByCell,
                                              LabelPropagationMethod method,
                                              LabelPropagationConfig config) {
        int errors = 0;
        int normals = 0;
        boolean anyCellConflict = false;
        double minDirectWeight = 1.0d;
        List<CellLabel> sources = new ArrayList<CellLabel>();
        for (ClusterAssignment member : members) {
            List<CellLabel> labels = labelsByCell.get(member.getCellId());
            if (labels == null || labels.isEmpty()) {
                continue;
            }
            Integer cellLabel = null;
            boolean currentCellConflict = false;
            for (CellLabel label : labels) {
                sources.add(label);
                minDirectWeight = Math.min(minDirectWeight, label.getSampleWeight());
                if (cellLabel == null) {
                    cellLabel = label.getLabel();
                } else if (cellLabel.intValue() != label.getLabel()) {
                    currentCellConflict = true;
                }
            }
            if (currentCellConflict) {
                anyCellConflict = true;
                continue;
            }
            if (cellLabel != null && cellLabel == 1) {
                errors++;
            } else if (cellLabel != null) {
                normals++;
            }
        }
        int directCount = errors + normals;
        if (sources.isEmpty()) {
            return PropagationDecision.noLabels();
        }
        if (anyCellConflict) {
            return PropagationDecision.conflict(errors, normals, sources, minDirectWeight);
        }
        int majority = Math.max(errors, normals);
        int conflictCount = Math.min(errors, normals);
        double majorityRatio = directCount == 0 ? 0.5d : (double) majority / directCount;
        if (method == LabelPropagationMethod.HOMOGENEITY && conflictCount > 0) {
            return PropagationDecision.conflict(errors, normals, sources, minDirectWeight);
        }
        if (method == LabelPropagationMethod.MAJORITY
                && (errors == normals || majorityRatio <= config.getMinimumMajorityRatio())) {
            return PropagationDecision.noMajority(
                    errors, normals, sources, minDirectWeight, majorityRatio);
        }
        return PropagationDecision.propagate(errors > normals ? 1 : 0,
                errors, normals, conflictCount, majorityRatio, sources, minDirectWeight);
    }

    private static List<CellLabel> propagatedLabels(
            List<ClusterAssignment> members,
            Map<String, List<CellLabel>> labelsByCell,
            PropagationDecision decision,
            LabelPropagationMethod method,
            LabelPropagationConfig config,
            long createdAt) {
        List<String> sourceIds = new ArrayList<String>();
        for (CellLabel source : decision.sources) {
            sourceIds.add(source.getLabelId());
        }
        Collections.sort(sourceIds);
        String sourceFingerprint = HashUtils.sha256Hex(String.join("|", sourceIds));
        List<CellLabel> propagated = new ArrayList<CellLabel>();
        for (ClusterAssignment member : members) {
            // 直接标签优先，传播只补充当前簇中尚未直接标注的单元格。
            if (labelsByCell.containsKey(member.getCellId())) {
                continue;
            }
            double confidence = decision.majorityRatio == null
                    ? 1.0d : decision.majorityRatio;
            double weight = Math.min(config.getPropagatedWeight() * confidence,
                    decision.minDirectWeight * 0.5d);
            String labelId = HashUtils.sha256Hex(member.getCellId() + "|"
                    + member.getClusterVersion() + "|" + member.getClusterId()
                    + "|" + method.name() + "|" + sourceFingerprint);
            propagated.add(new CellLabel(labelId, member.getCellId(), decision.label,
                    LabelSource.PROPAGATED, confidence, sourceFingerprint,
                    member.getClusterId(), member.getClusterVersion(), method,
                    weight, decision.conflictCount, decision.majorityRatio,
                    "label-propagation", createdAt));
        }
        return propagated;
    }

    private static String clusterKey(ClusterAssignment assignment) {
        return assignment.getColumnName() + "|" + assignment.getClusterVersion()
                + "|" + assignment.getClusterId();
    }

    private static final class PropagationDecision {
        /** 是否允许向未标注成员传播。 */
        private final boolean canPropagate;
        /** 传播标签值。 */
        private final int label;
        /** 直接标签总数。 */
        private final int directCount;
        /** 错误直接标签数。 */
        private final int errorCount;
        /** 正常直接标签数。 */
        private final int normalCount;
        /** 少数或冲突标签数。 */
        private final int conflictCount;
        /** 多数比例。 */
        private final Double majorityRatio;
        /** 传播状态。 */
        private final ClusterPropagationStatus status;
        /** 直接来源标签。 */
        private final List<CellLabel> sources;
        /** 当前簇直接标签最小权重。 */
        private final double minDirectWeight;

        private PropagationDecision(boolean canPropagate,
                                    int label,
                                    int errorCount,
                                    int normalCount,
                                    int conflictCount,
                                    Double majorityRatio,
                                    ClusterPropagationStatus status,
                                    List<CellLabel> sources,
                                    double minDirectWeight) {
            this.canPropagate = canPropagate;
            this.label = label;
            this.errorCount = errorCount;
            this.normalCount = normalCount;
            this.directCount = errorCount + normalCount;
            this.conflictCount = conflictCount;
            this.majorityRatio = majorityRatio;
            this.status = status;
            this.sources = sources;
            this.minDirectWeight = minDirectWeight;
        }

        private static PropagationDecision noLabels() {
            return new PropagationDecision(false, 0, 0, 0, 0, null,
                    ClusterPropagationStatus.NO_LABELS,
                    Collections.<CellLabel>emptyList(), 1.0d);
        }

        private static PropagationDecision conflict(int errors,
                                                    int normals,
                                                    List<CellLabel> sources,
                                                    double minWeight) {
            return new PropagationDecision(false, 0, errors, normals,
                    Math.max(1, Math.min(errors, normals)),
                    errors + normals == 0 ? null
                            : (double) Math.max(errors, normals) / (errors + normals),
                    ClusterPropagationStatus.CONFLICT, sources, minWeight);
        }

        private static PropagationDecision noMajority(int errors,
                                                      int normals,
                                                      List<CellLabel> sources,
                                                      double minWeight,
                                                      double ratio) {
            return new PropagationDecision(false, 0, errors, normals,
                    Math.min(errors, normals), ratio,
                    ClusterPropagationStatus.NO_MAJORITY, sources, minWeight);
        }

        private static PropagationDecision propagate(int label,
                                                     int errors,
                                                     int normals,
                                                     int conflicts,
                                                     double ratio,
                                                     List<CellLabel> sources,
                                                     double minWeight) {
            return new PropagationDecision(true, label, errors, normals,
                    conflicts, ratio, ClusterPropagationStatus.PROPAGATED,
                    sources, minWeight);
        }
    }
}
