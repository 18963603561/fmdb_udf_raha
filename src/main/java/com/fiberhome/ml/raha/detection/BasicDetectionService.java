package com.fiberhome.ml.raha.detection;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.data.ClassifierType;
import com.fiberhome.ml.raha.data.DetectionResult;
import com.fiberhome.ml.raha.data.StrategyFamily;
import com.fiberhome.ml.raha.error.RahaErrorCode;
import com.fiberhome.ml.raha.error.RahaException;
import com.fiberhome.ml.raha.feature.FeatureAssemblyResult;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.repository.ArtifactVersion;
import com.fiberhome.ml.raha.repository.DetectionResultRepository;
import com.fiberhome.ml.raha.strategy.StrategyHit;
import com.fiberhome.ml.raha.util.HashUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 使用可配置加权规则生成最终单元格检测判断并持久化。
 */
public final class BasicDetectionService {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(BasicDetectionService.class);
    /** 基础检测评分规则。 */
    private final DetectionScoringRule scoringRule;
    /** 检测结果仓储。 */
    private final DetectionResultRepository repository;
    /** 提供可测试检测时间的时钟。 */
    private final Clock clock;

    public BasicDetectionService(DetectionScoringRule scoringRule,
                                 DetectionResultRepository repository,
                                 Clock clock) {
        if (scoringRule == null || repository == null || clock == null) {
            throw new IllegalArgumentException("基础检测服务依赖不能为空");
        }
        this.scoringRule = scoringRule;
        this.repository = repository;
        this.clock = clock;
    }

    public DetectionBatchResult detectAndSave(String jobId,
                                              String configVersion,
                                              String stageId,
                                              FeatureAssemblyResult features,
                                              List<StrategyHit> hits,
                                              ModelConfig config,
                                              ArtifactVersion version) {
        if (features == null || hits == null || config == null || version == null) {
            throw new IllegalArgumentException("检测输入、配置和版本不能为空");
        }
        String modelName = modelName(config);
        String modelVersion = modelVersion(configVersion, config, modelName);
        Map<String, List<StrategyHit>> hitsByCell = indexHits(jobId, hits);
        List<DetectionResult> results = new ArrayList<DetectionResult>(features.getRows().size());
        long errorCount = 0L;
        double scoreSum = 0.0d;
        LOGGER.info("开始基础检测，jobId={}，stageId={}，cellCount={}，modelName={}",
                jobId, stageId, features.getRows().size(), modelName);
        for (SparseFeatureRow row : features.getRows()) {
            if (row.getCoordinate() == null || row.getValueHash() == null) {
                throw new IllegalStateException("检测要求特征包含单元格坐标和值哈希");
            }
            FeatureDictionary dictionary = features.getDictionaries().get(row.getColumnName());
            if (dictionary == null
                    || !dictionary.getVersion().equals(row.getFeatureDictionaryVersion())) {
                throw new IllegalStateException("特征向量与字典版本不一致");
            }
            List<StrategyHit> cellHits = hitsByCell.containsKey(row.getCellId())
                    ? hitsByCell.get(row.getCellId()) : Collections.<StrategyHit>emptyList();
            DetectionScore score = scoringRule.score(row, dictionary, cellHits, config);
            boolean error = score.getScore() >= config.getThreshold();
            if (error) {
                errorCount++;
            }
            scoreSum += score.getScore();
            results.add(new DetectionResult(jobId, configVersion, stageId,
                    row.getCoordinate(), row.getValueHash(), row.getMaskedValue(),
                    error, score.getScore(), config.getThreshold(), strategyIds(cellHits),
                    reasons(cellHits, score, modelName), modelName, modelVersion,
                    dictionary.getVersion(), clock.millis()));
        }
        DetectionMetrics metrics = new DetectionMetrics(results.size(), errorCount,
                results.isEmpty() ? 0.0d : scoreSum / results.size());
        repository.saveAll(jobId, results, version, clock.millis());
        LOGGER.info("基础检测完成，jobId={}，stageId={}，detectedCount={}，errorCount={}，averageScore={}",
                jobId, stageId, metrics.getDetectedCellCount(), metrics.getErrorCellCount(),
                metrics.getAverageScore());
        return new DetectionBatchResult(results, metrics);
    }

    private static String modelName(ModelConfig config) {
        if (config.getClassifierType() == ClassifierType.WEIGHTED_RULE) {
            return "weighted_rule";
        }
        if (config.isFallbackEnabled()) {
            return "fallback_weighted_rule";
        }
        throw new RahaException(RahaErrorCode.DETECTION_MODEL_UNAVAILABLE,
                "当前迭代未提供所选分类器且禁止规则降级", false);
    }

    private static String modelVersion(String configVersion,
                                       ModelConfig config,
                                       String modelName) {
        StringBuilder canonical = new StringBuilder();
        canonical.append(configVersion).append('|').append(modelName).append('|')
                .append(config.getThreshold()).append('|').append(config.getContextWeight());
        TreeMap<String, Double> weights = new TreeMap<String, Double>();
        for (Map.Entry<StrategyFamily, Double> entry : config.getStrategyFamilyWeights().entrySet()) {
            weights.put(entry.getKey().name(), entry.getValue());
        }
        for (Map.Entry<String, Double> entry : weights.entrySet()) {
            canonical.append('|').append(entry.getKey()).append('=').append(entry.getValue());
        }
        return HashUtils.sha256Hex(canonical.toString());
    }

    private static Map<String, List<StrategyHit>> indexHits(String jobId, List<StrategyHit> hits) {
        Map<String, List<StrategyHit>> index = new HashMap<String, List<StrategyHit>>();
        for (StrategyHit hit : hits) {
            if (!hit.getJobId().equals(jobId)) {
                throw new IllegalArgumentException("策略命中不属于当前检测任务");
            }
            String cellId = hit.getCoordinate().toCellId();
            if (!index.containsKey(cellId)) {
                index.put(cellId, new ArrayList<StrategyHit>());
            }
            index.get(cellId).add(hit);
        }
        return index;
    }

    private static List<String> strategyIds(List<StrategyHit> hits) {
        Set<String> ids = new LinkedHashSet<String>();
        for (StrategyHit hit : hits) {
            ids.add(hit.getStrategyId());
        }
        List<String> sorted = new ArrayList<String>(ids);
        Collections.sort(sorted);
        return sorted;
    }

    private static Map<String, String> reasons(List<StrategyHit> hits,
                                               DetectionScore score,
                                               String modelName) {
        List<StrategyHit> sortedHits = new ArrayList<StrategyHit>(hits);
        Collections.sort(sortedHits, new Comparator<StrategyHit>() {
            @Override
            public int compare(StrategyHit first, StrategyHit second) {
                int strategyCompare = first.getStrategyId().compareTo(second.getStrategyId());
                return strategyCompare == 0
                        ? first.getReasonCode().compareTo(second.getReasonCode()) : strategyCompare;
            }
        });
        Map<String, String> reasons = new LinkedHashMap<String, String>();
        reasons.put("modelMode", modelName);
        reasons.put("strategyScore", String.valueOf(score.getStrategyScore()));
        reasons.put("contextSignal", String.valueOf(score.getContextSignal()));
        for (Map.Entry<StrategyFamily, Double> entry : score.getFamilySignals().entrySet()) {
            reasons.put("familySignal." + entry.getKey().name().toLowerCase(Locale.ROOT),
                    String.valueOf(entry.getValue()));
        }
        for (int index = 0; index < sortedHits.size(); index++) {
            StrategyHit hit = sortedHits.get(index);
            reasons.put("strategyReason." + index,
                    hit.getStrategyFamily().name() + ":" + hit.getReasonCode()
                            + ":" + stableDetails(hit.getReasonDetails()));
        }
        return reasons;
    }

    private static String stableDetails(Map<String, String> details) {
        TreeMap<String, String> sorted = new TreeMap<String, String>(details);
        StringBuilder text = new StringBuilder();
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            if (text.length() > 0) {
                text.append(';');
            }
            text.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return text.toString();
    }
}
