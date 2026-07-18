package com.fiberhome.ml.raha.data.loader;

import com.fiberhome.ml.raha.data.ColumnMetadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据 Spark 模式和字段范围配置生成 Raha 字段元数据。
 */
public final class ColumnMetadataFactory {

    public List<ColumnMetadata> create(StructType schema, DataLoadRequest request) {
        if (schema == null || request == null) {
            throw new IllegalArgumentException("Spark 模式和数据加载请求不能为空");
        }
        List<ColumnMetadata> columns = new ArrayList<ColumnMetadata>();
        StructField[] fields = schema.fields();
        for (int index = 0; index < fields.length; index++) {
            StructField field = fields[index];
            boolean included = request.getIncludedColumns().isEmpty()
                    || request.getIncludedColumns().contains(field.name());
            boolean detectable = included
                    && !request.getExcludedColumns().contains(field.name())
                    && !request.getRowIdColumn().equals(field.name());
            boolean sensitive = request.getSensitiveColumns().contains(field.name());
            columns.add(new ColumnMetadata(field.name(), index,
                    field.dataType().catalogString(), field.nullable(), detectable, sensitive));
        }
        return Collections.unmodifiableList(columns);
    }
}

