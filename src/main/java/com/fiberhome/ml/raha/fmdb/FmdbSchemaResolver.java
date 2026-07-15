package com.fiberhome.ml.raha.fmdb;

import com.fiberhome.ml.raha.data.ColumnMetadata;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * 将 FMDB 返回的 Spark 模式解析为核心层字段元数据。
 */
public interface FmdbSchemaResolver {

    /**
     * 解析字段顺序、类型、可空、检测范围和敏感标记。
     *
     * @param schema FMDB Spark 模式
     * @param request 数据加载请求
     * @return 按模式顺序排列的字段元数据
     */
    List<ColumnMetadata> resolve(StructType schema, DataLoadRequest request);
}
