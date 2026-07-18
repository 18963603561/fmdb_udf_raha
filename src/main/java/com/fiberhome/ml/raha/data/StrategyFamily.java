package com.fiberhome.ml.raha.data;

/**
 * Raha 错误检测策略族。
 */
public enum StrategyFamily {
    /** 离群点检测。 */
    OD,
    /** 模式违规检测。 */
    PVD,
    /** 规则违规检测。 */
    RVD,
    /** 知识库违规检测。 */
    KBVD,
    /** 文本稀疏特征。 */
    TFIDF
}
