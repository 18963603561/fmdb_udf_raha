package com.fiberhome.ml.raha.udf;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * 描述 GenericUDF 返回结构中的一个字段。
 */
public final class RahaUdfField {

    /** 返回字段名称，必须与方案文档中的出参字段保持一致。 */
    private final String name;
    /** 返回字段类型，用于创建 Hive ObjectInspector。 */
    private final RahaUdfFieldType type;

    public RahaUdfField(String name, RahaUdfFieldType type) {
        if (name == null || name.trim().isEmpty() || type == null) {
            throw new IllegalArgumentException("UDF 返回字段名称和类型不能为空");
        }
        this.name = name;
        this.type = type;
    }

    public static RahaUdfField string(String name) {
        return new RahaUdfField(name, RahaUdfFieldType.STRING);
    }

    public static RahaUdfField longField(String name) {
        return new RahaUdfField(name, RahaUdfFieldType.LONG);
    }

    public static RahaUdfField intField(String name) {
        return new RahaUdfField(name, RahaUdfFieldType.INT);
    }

    public static RahaUdfField doubleField(String name) {
        return new RahaUdfField(name, RahaUdfFieldType.DOUBLE);
    }

    public static RahaUdfField booleanField(String name) {
        return new RahaUdfField(name, RahaUdfFieldType.BOOLEAN);
    }

    public String getName() {
        return name;
    }

    public RahaUdfFieldType getType() {
        return type;
    }

    public ObjectInspector objectInspector() {
        return type.objectInspector();
    }
}
