package com.fiberhome.ml.raha.detection;

import com.fiberhome.ml.raha.config.ModelConfig;
import com.fiberhome.ml.raha.feature.FeatureDictionary;
import com.fiberhome.ml.raha.feature.SparseFeatureRow;
import com.fiberhome.ml.raha.strategy.StrategyHit;

import java.util.List;

/**
 * 基础检测评分规则契约，输入特征和策略命中，输出可解释分数。
 */
public interface DetectionScoringRule {

    DetectionScore score(SparseFeatureRow row,
                         FeatureDictionary dictionary,
                         List<StrategyHit> hits,
                         ModelConfig config);
}
