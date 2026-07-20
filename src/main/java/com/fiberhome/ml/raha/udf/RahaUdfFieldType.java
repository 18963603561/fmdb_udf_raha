package com.fiberhome.ml.raha.udf;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 定义 GenericUDF 二维表返回字段支持的基础类型。
 */
public enum RahaUdfFieldType {
    /** 字符串字段。 */
    STRING {
        @Override
        ObjectInspector objectInspector() {
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }
    },
    /** 长整数字段。 */
    LONG {
        @Override
        ObjectInspector objectInspector() {
            return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        }
    },
    /** 整数字段。 */
    INT {
        @Override
        ObjectInspector objectInspector() {
            return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        }
    },
    /** 双精度数字字段。 */
    DOUBLE {
        @Override
        ObjectInspector objectInspector() {
            return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        }
    },
    /** 布尔字段。 */
    BOOLEAN {
        @Override
        ObjectInspector objectInspector() {
            return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        }
    };

    abstract ObjectInspector objectInspector();
}
