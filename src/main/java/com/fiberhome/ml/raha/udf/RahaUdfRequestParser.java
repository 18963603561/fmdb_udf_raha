package com.fiberhome.ml.raha.udf;

import com.fiberhome.ml.raha.config.RahaDefaultConfigProvider;
import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.service.RahaTaskType;
import com.fiberhome.ml.raha.util.FormDataCodec;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.io.Serializable;

/**
 * 解析严格表单编码的 UDF 请求并拒绝未知或跨任务参数。
 */
public final class RahaUdfRequestParser implements Serializable {

    /** Java 序列化版本。 */
    private static final long serialVersionUID = 1L;
    /** 单次 UDF 请求允许的最大字符数。 */
    private final int maxRequestLength;

    /**
     * UDF 请求允许出现的表单字段白名单，用于拒绝拼写错误、无效或跨任务传入的参数。
     *
     * <p>通用字段：</p>
     * <ul>
     *     <li><code>datasetId</code>：逻辑数据集标识；必填；适用于训练、检测和采样任务。
     *         样例：<code>datasetId=orders_202607</code>。</li>
     *     <li><code>inputReference</code>：待处理的 FMDB 表名或只读 SQL；必填；适用于训练、检测和采样任务。
     *         样例：<code>inputReference=ods.orders_dirty</code>。</li>
     *     <li><code>sourceType</code>：输入来源类型，只允许 <code>TABLE</code> 或 <code>SQL</code>；必填；
     *         用于确定 <code>inputReference</code> 按表名还是 SQL 解析。
     *         样例：<code>sourceType=TABLE</code>。</li>
     *     <li><code>rowIdColumn</code>：输入数据中唯一且稳定的行标识字段；必填；适用于训练、检测和采样任务。
     *         样例：<code>rowIdColumn=id</code>。</li>
     *     <li><code>snapshotId</code>：输入数据快照标识；选填；用于训练、检测或采样时固定并追溯同一批数据。
     *         样例：<code>snapshotId=orders_snapshot_001</code>。</li>
     *     <li><code>idempotencyKey</code>：调用方生成的幂等键；必填；用于重试提交时复用已创建的同配置任务。
     *         样例：<code>idempotencyKey=detect_orders_202607_v1</code>。</li>
     *     <li><code>caller</code>：调用方或调用人标识；必填；用于任务审计和日志追踪。
     *         样例：<code>caller=data_quality</code>。</li>
     *     <li><code>resultTable</code>：写入任务产物或结果的 FMDB 表名；必填；适用于训练、检测和采样任务。
     *         样例：<code>resultTable=dw.raha_detection_result</code>。</li>
     * </ul>
     *
     * <p>任务专属字段：</p>
     * <ul>
     *     <li><code>annotationReference</code>：人工标注数据的引用；训练任务必填，检测和采样任务禁止传入；
     *         用于为模型训练提供标签。
     *         样例：<code>annotationReference=dw.raha_labels</code>。</li>
     *     <li><code>modelVersion</code>：用于检测的已发布模型版本；检测任务必填，训练和采样任务禁止传入；
     *         用于指定本次检测加载的模型。
     *         样例：<code>modelVersion=orders_model_v1</code>。</li>
     *     <li><code>labelingBudget</code>：待人工标注的最大行数，必须为正整数；采样任务必填，训练和检测任务禁止传入；
     *         用于控制一次采样生成的标注工作量。
     *         样例：<code>labelingBudget=20</code>。</li>
     * </ul>
     */
    private static final Set<String> ALLOWED_KEYS = new LinkedHashSet<String>(Arrays.asList(
            "datasetId", "inputReference", "sourceType", "rowIdColumn", "snapshotId",
            "idempotencyKey", "caller", "resultTable", "annotationReference",
            "modelVersion", "labelingBudget"));

    public RahaUdfRequestParser() {
        this(RahaDefaultConfigProvider.factory().udfConfig().getMaxRequestLength());
    }

    public RahaUdfRequestParser(int maxRequestLength) {
        if (maxRequestLength <= 0) {
            throw new IllegalArgumentException("UDF 请求长度上限必须大于 0");
        }
        this.maxRequestLength = maxRequestLength;
    }

    public RahaUdfRequest parse(RahaTaskType taskType, String encodedRequest) {
        if (encodedRequest != null && encodedRequest.length() > maxRequestLength) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 请求长度超过上限");
        }
        Map<String, String> values;
        try {
            values = FormDataCodec.decode(encodedRequest);
        } catch (IllegalArgumentException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "UDF 请求不是合法表单编码", exception);
        }
        for (String key : values.keySet()) {
            if (!ALLOWED_KEYS.contains(key)) {
                throw new RahaUdfException("UNKNOWN_UDF_ARGUMENT",
                        "UDF 请求包含未知参数：" + key);
            }
        }
        DataFormat sourceType = sourceType(values.get("sourceType"));
        int budget = integerValue(values.get("labelingBudget"));
        return new RahaUdfRequest(taskType, values.get("datasetId"),
                values.get("inputReference"), sourceType, values.get("rowIdColumn"),
                values.get("snapshotId"), values.get("idempotencyKey"),
                values.get("caller"), values.get("resultTable"),
                values.get("annotationReference"), values.get("modelVersion"), budget);
    }

    private static DataFormat sourceType(String value) {
        if ("TABLE".equalsIgnoreCase(value)) {
            return DataFormat.FMDB_TABLE;
        }
        if ("SQL".equalsIgnoreCase(value)) {
            return DataFormat.FMDB_SQL;
        }
        throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                "sourceType 只允许 TABLE 或 SQL");
    }

    private static int integerValue(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new RahaUdfException("INVALID_UDF_ARGUMENT",
                    "labelingBudget 必须为整数", exception);
        }
    }
}
