package com.fiberhome.ml.raha.service.task.batch;

import com.fiberhome.ml.raha.data.loader.DataFormat;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 只读取 Spark 输入模式并解析列批目标字段，不触发完整数据加载阶段。
 */
public final class ColumnBatchSchemaResolver {

    /** 日志记录器。 */
    private static final Logger LOGGER = LoggerFactory.getLogger(
            ColumnBatchSchemaResolver.class);
    /** 由外部运行时管理生命周期的 Spark 会话。 */
    private final SparkSession sparkSession;

    public ColumnBatchSchemaResolver(SparkSession sparkSession) {
        if (sparkSession == null) {
            throw new IllegalArgumentException("列批模式解析 Spark 会话不能为空");
        }
        this.sparkSession = sparkSession;
    }

    /**
     * 解析当前请求真正可检测的业务字段。
     *
     * @param request 数据加载请求
     * @return 按输入模式顺序排列的字段列表
     */
    public List<String> resolve(DataLoadRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("列批数据加载请求不能为空");
        }
        LOGGER.info("开始解析列批输入模式，datasetId={}，format={}，inputReference={}",
                request.getDatasetId(), request.getFormat(),
                request.getSourceReference());
        StructType schema;
        if (request.getFormat() == DataFormat.FMDB_TABLE) {
            schema = sparkSession.table(request.getInputReference()).schema();
        } else if (request.getFormat() == DataFormat.FMDB_SQL) {
            schema = sparkSession.sql(request.getInputReference()).schema();
        } else {
            throw new IllegalArgumentException("列批入口只支持 FMDB 表或只读 SQL");
        }
        Set<String> available = new LinkedHashSet<String>();
        for (StructField field : schema.fields()) {
            available.add(field.name());
        }
        Set<String> missing = new LinkedHashSet<String>(
                request.getIncludedColumns());
        missing.removeAll(available);
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("列批白名单包含不存在字段：" + missing);
        }
        List<String> result = new ArrayList<String>();
        for (StructField field : schema.fields()) {
            String column = field.name();
            boolean included = request.getIncludedColumns().isEmpty()
                    || request.getIncludedColumns().contains(column);
            if (included && !request.getExcludedColumns().contains(column)
                    && !request.getRowIdentityConfig().getKeyColumns()
                    .contains(column)) {
                result.add(column);
            }
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("列批输入没有可处理业务字段");
        }
        LOGGER.info("列批输入模式解析完成，datasetId={}，targetColumnCount={}",
                request.getDatasetId(), result.size());
        return result;
    }
}
