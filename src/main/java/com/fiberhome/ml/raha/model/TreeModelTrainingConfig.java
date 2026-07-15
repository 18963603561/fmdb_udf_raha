package com.fiberhome.ml.raha.model;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;

/**
 * 控制决策树和梯度提升树训练边界的配置。
 */
public final class TreeModelTrainingConfig {

    /** 决策树及 GBT 单棵树的最大深度。 */
    private final int maxDepth;
    /** 特征离散化允许的最大桶数量。 */
    private final int maxBins;
    /** 节点继续分裂所需的最小样本数。 */
    private final int minInstancesPerNode;
    /** 节点分裂所需的最小信息增益。 */
    private final double minInfoGain;
    /** GBT 最大迭代次数和树数量。 */
    private final int maxIterations;
    /** GBT 每轮树模型的步长。 */
    private final double stepSize;
    /** GBT 每轮训练使用的样本比例。 */
    private final double subsamplingRate;

    public TreeModelTrainingConfig(int maxDepth,
                                   int maxBins,
                                   int minInstancesPerNode,
                                   double minInfoGain,
                                   int maxIterations,
                                   double stepSize,
                                   double subsamplingRate) {
        if (maxDepth < 0 || maxBins < 2 || minInstancesPerNode <= 0
                || Double.isNaN(minInfoGain) || Double.isInfinite(minInfoGain)
                || minInfoGain < 0.0d || maxIterations <= 0
                || Double.isNaN(stepSize) || Double.isInfinite(stepSize)
                || stepSize <= 0.0d || Double.isNaN(subsamplingRate)
                || Double.isInfinite(subsamplingRate) || subsamplingRate <= 0.0d
                || subsamplingRate > 1.0d) {
            throw new IllegalArgumentException("树模型训练配置非法");
        }
        this.maxDepth = maxDepth;
        this.maxBins = maxBins;
        this.minInstancesPerNode = minInstancesPerNode;
        this.minInfoGain = minInfoGain;
        this.maxIterations = maxIterations;
        this.stepSize = stepSize;
        this.subsamplingRate = subsamplingRate;
    }

    public static TreeModelTrainingConfig defaults() {
        return RahaDefaultConfigProvider.factory().treeModelTrainingConfig();
    }

    public int getMaxDepth() { return maxDepth; }
    public int getMaxBins() { return maxBins; }
    public int getMinInstancesPerNode() { return minInstancesPerNode; }
    public double getMinInfoGain() { return minInfoGain; }
    public int getMaxIterations() { return maxIterations; }
    public double getStepSize() { return stepSize; }
    public double getSubsamplingRate() { return subsamplingRate; }
}
