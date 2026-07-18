package com.fiberhome.ml.raha.sampling;

import com.fiberhome.ml.raha.sampling.domain.TupleSamplingScore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * 使用固定随机种子的加权无放回算法选择待标注元组。
 */
public final class TupleSampler {

    /**
     * 根据元组权重执行预算内无放回采样。
     *
     * @param scores 元组采样分数
     * @param budget 最大选择数量
     * @param randomSeed 可复现随机种子
     * @return 不重复的选中元组
     */
    public List<TupleSamplingScore> select(List<TupleSamplingScore> scores,
                                           int budget,
                                           long randomSeed) {
        if (scores == null || budget <= 0) {
            throw new IllegalArgumentException("采样分数和预算必须有效");
        }
        List<TupleSamplingScore> sortedScores = new ArrayList<TupleSamplingScore>(scores);
        Collections.sort(sortedScores, Comparator.comparing(TupleSamplingScore::getRowId));
        Random random = new Random(randomSeed);
        List<WeightedCandidate> candidates = new ArrayList<WeightedCandidate>();
        for (TupleSamplingScore score : sortedScores) {
            double uniform = Math.max(Double.MIN_VALUE, random.nextDouble());
            double key = -Math.log(uniform) / Math.max(Double.MIN_VALUE, score.getScore());
            candidates.add(new WeightedCandidate(score, key));
        }
        Collections.sort(candidates);
        List<TupleSamplingScore> selected = new ArrayList<TupleSamplingScore>();
        int limit = Math.min(budget, candidates.size());
        for (int index = 0; index < limit; index++) {
            selected.add(candidates.get(index).score);
        }
        return Collections.unmodifiableList(selected);
    }

    private static final class WeightedCandidate implements Comparable<WeightedCandidate> {
        /** 元组覆盖分数。 */
        private final TupleSamplingScore score;
        /** 加权无放回排序键，数值越小越优先。 */
        private final double key;

        private WeightedCandidate(TupleSamplingScore score, double key) {
            this.score = score;
            this.key = key;
        }

        @Override
        public int compareTo(WeightedCandidate other) {
            int keyCompare = Double.compare(key, other.key);
            return keyCompare == 0 ? score.getRowId().compareTo(other.score.getRowId())
                    : keyCompare;
        }
    }
}
