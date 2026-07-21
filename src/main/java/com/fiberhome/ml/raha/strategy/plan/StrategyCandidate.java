package com.fiberhome.ml.raha.strategy.plan;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 策略发现的单元格候选信号，不包含原始值和纠正值。
 */
public final class StrategyCandidate {

    /** 稳定行标识。 */
    private final String rowId;
    /** 候选字段名称。 */
    private final String columnName;
    /** 原始值哈希。 */
    private final String valueHash;
    /** 稳定原因编码。 */
    private final String reasonCode;
    /** 原因参数。 */
    private final Map<String, String> reasonDetails;
    /** 候选分数。 */
    private final Double score;

    public StrategyCandidate(String rowId,
                             String columnName,
                             String valueHash,
                             String reasonCode,
                             Map<String, String> reasonDetails,
                             Double score) {
        this.rowId = ValueUtils.requireNotBlank(rowId, "行标识");
        this.columnName = ValueUtils.requireNotBlank(columnName, "字段名称");
        this.valueHash = ValueUtils.requireNotBlank(valueHash, "值哈希");
        if (!valueHash.matches("[0-9a-f]{32}")) {
            throw new IllegalArgumentException("候选值必须使用 MD5 哈希");
        }
        this.reasonCode = ValueUtils.requireNotBlank(reasonCode, "原因编码");
        this.reasonDetails = reasonDetails == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(reasonDetails));
        if (score != null && (Double.isNaN(score) || Double.isInfinite(score)
                || score < 0.0d || score > 1.0d)) {
            throw new IllegalArgumentException("候选分数必须位于 0 到 1 之间");
        }
        this.score = score;
    }

    public String getRowId() {
        return rowId;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getValueHash() {
        return valueHash;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    public Map<String, String> getReasonDetails() {
        return reasonDetails;
    }

    public Double getScore() {
        return score;
    }
}
