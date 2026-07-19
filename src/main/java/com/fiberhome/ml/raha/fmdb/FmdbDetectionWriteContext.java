package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 保存最终错误结果入库所需的检测批次和受信任原始行上下文。
 */
public final class FmdbDetectionWriteContext {

    /** 检测批次标识。 */
    private final String detectionBatchId;
    /** 检测输入引用。 */
    private final String inputReference;
    /** 使用的模型集合版本。 */
    private final String modelSetVersion;
    /** 按逻辑行标识索引的原始业务行。 */
    private final Map<String, Map<String, Object>> rowsById;

    public FmdbDetectionWriteContext(String detectionBatchId,
                                     String inputReference,
                                     String modelSetVersion,
                                     Map<String, Map<String, Object>> rowsById) {
        this.detectionBatchId = ValueUtils.requireNotBlank(
                detectionBatchId, "检测批次标识");
        this.inputReference = ValueUtils.requireNotBlank(
                inputReference, "检测输入引用");
        this.modelSetVersion = ValueUtils.requireNotBlank(
                modelSetVersion, "模型集合版本");
        if (rowsById == null) {
            throw new IllegalArgumentException("检测原始行索引不能为空");
        }
        Map<String, Map<String, Object>> copied =
                new LinkedHashMap<String, Map<String, Object>>();
        for (Map.Entry<String, Map<String, Object>> entry : rowsById.entrySet()) {
            String rowId = ValueUtils.requireNotBlank(entry.getKey(), "检测行标识");
            if (entry.getValue() == null) {
                throw new IllegalArgumentException("检测原始行不能为空：" + rowId);
            }
            copied.put(rowId, Collections.unmodifiableMap(
                    new LinkedHashMap<String, Object>(entry.getValue())));
        }
        this.rowsById = Collections.unmodifiableMap(copied);
    }

    public String getDetectionBatchId() {
        return detectionBatchId;
    }

    public String getInputReference() {
        return inputReference;
    }

    public String getModelSetVersion() {
        return modelSetVersion;
    }

    public Map<String, Object> requireRow(String rowId) {
        Map<String, Object> row = rowsById.get(rowId);
        if (row == null) {
            throw new IllegalArgumentException("检测结果缺少受信任原始行：" + rowId);
        }
        return row;
    }
}
