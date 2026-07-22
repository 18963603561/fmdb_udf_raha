package com.fiberhome.ml.raha.repository.port;

import com.fiberhome.ml.raha.data.domain.RahaDataset;
import com.fiberhome.ml.raha.util.ValueUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * 提供检测结果写入最终错误表所需的可信数据集和模型集合线索。
 */
public final class DetectionResultSaveContext {

    /** 实际产生检测结果的子任务标识。 */
    private final String jobId;
    /** 最终结果归属的父级检测批次标识。 */
    private final String detectionBatchId;
    /** 包含可信原始行和输入引用的数据集。 */
    private final RahaDataset dataset;
    /** 当前检测实际使用的列模型版本。 */
    private final List<String> modelVersions;
    /** 调用方显式选择的模型集合版本，旧入口为空。 */
    private final String modelSetVersion;

    public DetectionResultSaveContext(String jobId,
                                      RahaDataset dataset,
                                      Collection<String> modelVersions) {
        this(jobId, jobId, dataset, null, modelVersions);
    }

    public DetectionResultSaveContext(String jobId,
                                      RahaDataset dataset,
                                      String modelSetVersion,
                                      Collection<String> modelVersions) {
        this(jobId, jobId, dataset, modelSetVersion, modelVersions);
    }

    public DetectionResultSaveContext(String jobId,
                                      String detectionBatchId,
                                      RahaDataset dataset,
                                      String modelSetVersion,
                                      Collection<String> modelVersions) {
        this.jobId = ValueUtils.requireNotBlank(jobId, "检测任务标识");
        this.detectionBatchId = ValueUtils.requireNotBlank(
                detectionBatchId, "检测批次标识");
        if (dataset == null || dataset.getDataFrame() == null
                || modelVersions == null || modelVersions.isEmpty()) {
            throw new IllegalArgumentException("检测数据集和模型版本不能为空");
        }
        List<String> copies = new ArrayList<String>(modelVersions.size());
        for (String modelVersion : modelVersions) {
            copies.add(ValueUtils.requireNotBlank(modelVersion, "检测模型版本"));
        }
        this.dataset = dataset;
        this.modelVersions = Collections.unmodifiableList(copies);
        this.modelSetVersion = modelSetVersion == null
                || modelSetVersion.trim().isEmpty()
                ? null : modelSetVersion.trim();
    }

    public String getJobId() {
        return jobId;
    }

    public RahaDataset getDataset() {
        return dataset;
    }

    public String getDetectionBatchId() {
        return detectionBatchId;
    }

    public List<String> getModelVersions() {
        return modelVersions;
    }

    public String getModelSetVersion() {
        return modelSetVersion;
    }
}
