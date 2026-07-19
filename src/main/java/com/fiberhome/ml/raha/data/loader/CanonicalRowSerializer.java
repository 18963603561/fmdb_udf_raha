package com.fiberhome.ml.raha.data.loader;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

/**
 * 按字段名、类型和值生成无分隔符歧义的规范行文本。
 */
public final class CanonicalRowSerializer implements Serializable {

    private static final long serialVersionUID = 1L;
    /** 待序列化字段及其固定顺序。 */
    private final StructType schema;
    /** 规范化协议版本。 */
    private final String normalizationVersion;

    public CanonicalRowSerializer(StructType schema,
                                  String normalizationVersion) {
        if (schema == null || schema.fields().length == 0) {
            throw new IllegalArgumentException("规范行模式不能为空");
        }
        if (!RowIdentityConfig.NORMALIZATION_VERSION.equals(
                normalizationVersion)) {
            throw new IllegalArgumentException("不支持的行身份规范版本："
                    + normalizationVersion);
        }
        this.schema = schema;
        this.normalizationVersion = normalizationVersion;
    }

    /**
     * 生成包含协议、字段名、类型和值的规范文本。
     *
     * @param row 字段顺序与构造模式一致的 Spark 行
     * @return 可稳定计算哈希的规范文本
     */
    public String serialize(Row row) {
        if (row == null || row.size() != schema.fields().length) {
            throw new IllegalArgumentException("规范行字段数量与模式不一致");
        }
        StringBuilder text = new StringBuilder();
        appendToken(text, normalizationVersion);
        appendToken(text, String.valueOf(schema.fields().length));
        for (int index = 0; index < schema.fields().length; index++) {
            StructField field = schema.fields()[index];
            appendToken(text, field.name());
            appendToken(text, field.dataType().catalogString());
            appendToken(text, serializeValue(row.get(index), field.dataType()));
        }
        return text.toString();
    }

    private static String serializeValue(Object value, DataType dataType) {
        if (value == null) {
            return "N";
        }
        if (dataType.equals(DataTypes.StringType)) {
            return "S" + token(String.valueOf(value));
        }
        if (dataType.equals(DataTypes.BooleanType)) {
            return Boolean.TRUE.equals(value) ? "B1" : "B0";
        }
        if (dataType.equals(DataTypes.ByteType)
                || dataType.equals(DataTypes.ShortType)
                || dataType.equals(DataTypes.IntegerType)
                || dataType.equals(DataTypes.LongType)) {
            return "I" + String.valueOf(value);
        }
        if (dataType.equals(DataTypes.FloatType)
                || dataType.equals(DataTypes.DoubleType)) {
            return "F" + floating((Number) value);
        }
        if (dataType instanceof org.apache.spark.sql.types.DecimalType) {
            BigDecimal decimal = value instanceof Decimal
                    ? ((Decimal) value).toJavaBigDecimal()
                    : (BigDecimal) value;
            return "M" + decimal.stripTrailingZeros().toPlainString();
        }
        if (dataType.equals(DataTypes.DateType)) {
            return "D" + ((Date) value).toLocalDate().toString();
        }
        if (dataType.equals(DataTypes.TimestampType)) {
            return "T" + ((Timestamp) value).toInstant().toString();
        }
        if (dataType.equals(DataTypes.BinaryType)) {
            return "Y" + Base64.getEncoder().encodeToString((byte[]) value);
        }
        if (dataType instanceof ArrayType) {
            return serializeArray(value, (ArrayType) dataType);
        }
        if (dataType instanceof MapType) {
            return serializeMap(value, (MapType) dataType);
        }
        if (dataType instanceof StructType && value instanceof Row) {
            return "R" + new CanonicalRowSerializer((StructType) dataType,
                    RowIdentityConfig.NORMALIZATION_VERSION).serialize((Row) value);
        }
        // Spark 区间等稳定标量类型采用类型名和标准字符串，避免静默丢字段。
        return "X" + token(dataType.catalogString())
                + token(String.valueOf(value));
    }

    private static String serializeArray(Object value, ArrayType arrayType) {
        List<Object> elements = new ArrayList<Object>();
        if (value instanceof Seq) {
            Iterator<?> iterator = ((Seq<?>) value).iterator();
            while (iterator.hasNext()) {
                elements.add(iterator.next());
            }
        } else if (value instanceof List) {
            elements.addAll((List<?>) value);
        } else if (value instanceof Object[]) {
            Collections.addAll(elements, (Object[]) value);
        } else {
            throw new IllegalArgumentException("不支持的 Spark 数组值类型："
                    + value.getClass().getName());
        }
        StringBuilder text = new StringBuilder("A");
        appendToken(text, String.valueOf(elements.size()));
        for (Object element : elements) {
            appendToken(text, serializeValue(element, arrayType.elementType()));
        }
        return text.toString();
    }

    private static String serializeMap(Object value, MapType mapType) {
        List<String> entries = new ArrayList<String>();
        if (value instanceof scala.collection.Map) {
            Iterator<?> iterator = ((scala.collection.Map<?, ?>) value).iterator();
            while (iterator.hasNext()) {
                Tuple2<?, ?> entry = (Tuple2<?, ?>) iterator.next();
                entries.add(mapEntry(entry._1(), entry._2(), mapType));
            }
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                entries.add(mapEntry(entry.getKey(), entry.getValue(), mapType));
            }
        } else {
            throw new IllegalArgumentException("不支持的 Spark 映射值类型："
                    + value.getClass().getName());
        }
        Collections.sort(entries);
        StringBuilder text = new StringBuilder("P");
        appendToken(text, String.valueOf(entries.size()));
        for (String entry : entries) {
            appendToken(text, entry);
        }
        return text.toString();
    }

    private static String mapEntry(Object key, Object value, MapType type) {
        return token(serializeValue(key, type.keyType()))
                + token(serializeValue(value, type.valueType()));
    }

    private static String floating(Number number) {
        double value = number.doubleValue();
        if (Double.isNaN(value)) {
            return "NaN";
        }
        if (Double.isInfinite(value)) {
            return value > 0.0d ? "+Infinity" : "-Infinity";
        }
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }

    private static void appendToken(StringBuilder text, String value) {
        text.append(token(value));
    }

    private static String token(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return bytes.length + ":" + value;
    }
}
