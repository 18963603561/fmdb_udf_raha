package com.fiberhome.ml.raha.udf;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 集中定义三个检测 UDF 的二维表返回字段。
 */
public final class RahaUdfFields {

    /** 三个函数共享的公共输出字段。 */
    private static final List<RahaUdfField> COMMON = Collections.unmodifiableList(
            Arrays.asList(
                    RahaUdfField.string("status"),
                    RahaUdfField.string("errorCode"),
                    RahaUdfField.string("errorMessage"),
                    RahaUdfField.string("datasetId"),
                    RahaUdfField.string("snapshotId"),
                    RahaUdfField.string("sourceType"),
                    RahaUdfField.string("inputReference"),
                    RahaUdfField.longField("createdAt"),
                    RahaUdfField.string("jobId"),
                    RahaUdfField.string("configVersion"),
                    RahaUdfField.string("idempotentKey"),
                    RahaUdfField.booleanField("reused"),
                    RahaUdfField.booleanField("forceRun"),
                    RahaUdfField.string("forceRunId"),
                    RahaUdfField.string("baseExecutionInputFingerprint"),
                    RahaUdfField.string("executionInputFingerprint"),
                    RahaUdfField.string("currentSelectRule")));

    /** 采集函数输出字段。 */
    public static final List<RahaUdfField> COLLECT = append(COMMON,
            RahaUdfField.string("sourceVersion"),
            RahaUdfField.string("schemaHash"),
            RahaUdfField.longField("rowCount"),
            RahaUdfField.intField("fieldCount"),
            RahaUdfField.intField("validFieldCount"),
            RahaUdfField.string("sampleBatchId"),
            RahaUdfField.longField("sampleRecordCount"),
            RahaUdfField.longField("annotationTaskCount"),
            RahaUdfField.intField("clusterCount"),
            RahaUdfField.intField("clusteredFieldCount"),
            RahaUdfField.string("annotationExcelName"),
            RahaUdfField.string("annotationZipName"),
            RahaUdfField.string("annotationZipUrl"),
            RahaUdfField.string("partitionMonth"));

    /** 训练函数输出字段。 */
    public static final List<RahaUdfField> TRAIN = append(COMMON,
            RahaUdfField.string("sampleBatchId"),
            RahaUdfField.string("annotationBatchId"),
            RahaUdfField.string("annotationFileName"),
            RahaUdfField.string("annotationStatus"),
            RahaUdfField.longField("annotationRecordCount"),
            RahaUdfField.longField("validAnnotationCount"),
            RahaUdfField.longField("invalidAnnotationCount"),
            RahaUdfField.string("modelSetVersion"),
            RahaUdfField.string("columnName"),
            RahaUdfField.string("modelVersion"),
            RahaUdfField.string("modelStatus"),
            RahaUdfField.string("classifierType"),
            RahaUdfField.string("featureDictionaryVersion"),
            RahaUdfField.string("strategyPlanVersion"),
            RahaUdfField.doubleField("threshold"),
            RahaUdfField.string("metricJson"),
            RahaUdfField.string("reportZipName"),
            RahaUdfField.string("reportZipUrl"));

    /** 检测函数输出字段。 */
    public static final List<RahaUdfField> DETECT = append(COMMON,
            RahaUdfField.string("modelSetVersion"),
            RahaUdfField.string("schemaHash"),
            RahaUdfField.longField("rowCount"),
            RahaUdfField.intField("fieldCount"),
            RahaUdfField.intField("validFieldCount"),
            RahaUdfField.intField("modelFieldCount"),
            RahaUdfField.intField("failedFieldCount"),
            RahaUdfField.longField("detectedCellCount"),
            RahaUdfField.longField("detectedErrorCount"),
            RahaUdfField.string("resultTable"),
            RahaUdfField.string("detailZipName"),
            RahaUdfField.string("detailZipUrl"));

    private RahaUdfFields() {
    }

    private static List<RahaUdfField> append(List<RahaUdfField> base,
                                             RahaUdfField... extra) {
        java.util.ArrayList<RahaUdfField> fields =
                new java.util.ArrayList<RahaUdfField>(base);
        fields.addAll(Arrays.asList(extra));
        return Collections.unmodifiableList(fields);
    }
}
