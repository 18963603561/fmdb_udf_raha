package com.fiberhome.ml.raha.label;

/**
 * 控制传播标签权重和多数传播最低比例。
 */
public final class LabelPropagationConfig {

    /** 传播标签相对直接标签的最大基础权重。 */
    private final double propagatedWeight;
    /** 多数传播允许执行的最低多数比例。 */
    private final double minimumMajorityRatio;

    public LabelPropagationConfig(double propagatedWeight,
                                  double minimumMajorityRatio) {
        if (Double.isNaN(propagatedWeight) || propagatedWeight <= 0.0d
                || propagatedWeight >= 1.0d
                || Double.isNaN(minimumMajorityRatio)
                || minimumMajorityRatio < 0.5d || minimumMajorityRatio > 1.0d) {
            throw new IllegalArgumentException("传播权重和多数比例配置非法");
        }
        this.propagatedWeight = propagatedWeight;
        this.minimumMajorityRatio = minimumMajorityRatio;
    }

    public static LabelPropagationConfig defaults() {
        return new LabelPropagationConfig(0.5d, 0.5d);
    }

    public double getPropagatedWeight() { return propagatedWeight; }
    public double getMinimumMajorityRatio() { return minimumMajorityRatio; }
}
