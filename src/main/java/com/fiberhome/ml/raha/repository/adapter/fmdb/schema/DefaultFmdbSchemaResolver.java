package com.fiberhome.ml.raha.repository.adapter.fmdb.schema;

import com.fiberhome.ml.raha.data.domain.ColumnMetadata;
import com.fiberhome.ml.raha.data.loader.metadata.ColumnMetadataFactory;
import com.fiberhome.ml.raha.data.loader.DataLoadRequest;
import java.util.List;
import org.apache.spark.sql.types.StructType;

/**
 * 复用核心字段规则解析 FMDB Spark 模式，避免适配层改变检测字段语义。
 */
public final class DefaultFmdbSchemaResolver implements FmdbSchemaResolver {

    /** 核心字段元数据工厂。 */
    private final ColumnMetadataFactory metadataFactory;

    public DefaultFmdbSchemaResolver(ColumnMetadataFactory metadataFactory) {
        if (metadataFactory == null) {
            throw new IllegalArgumentException("字段元数据工厂不能为空");
        }
        this.metadataFactory = metadataFactory;
    }

    @Override
    public List<ColumnMetadata> resolve(StructType schema, DataLoadRequest request) {
        return metadataFactory.create(schema, request);
    }
}
