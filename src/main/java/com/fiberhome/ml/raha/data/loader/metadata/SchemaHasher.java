package com.fiberhome.ml.raha.data.loader.metadata;

import com.fiberhome.ml.raha.util.HashUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 根据字段顺序、名称、类型和可空性生成稳定模式哈希。
 */
public final class SchemaHasher {

    public String hash(StructType schema) {
        if (schema == null || schema.fields().length == 0) {
            throw new IllegalArgumentException("Spark 模式不能为空");
        }
        StringBuilder builder = new StringBuilder();
        for (StructField field : schema.fields()) {
            append(builder, field.name());
            append(builder, field.dataType().catalogString());
            append(builder, String.valueOf(field.nullable()));
        }
        return HashUtils.md5Hex(builder.toString());
    }

    private static void append(StringBuilder builder, String value) {
        builder.append(value.length()).append(':').append(value);
    }
}

