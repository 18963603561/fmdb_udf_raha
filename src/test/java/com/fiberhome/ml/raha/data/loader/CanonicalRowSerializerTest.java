package com.fiberhome.ml.raha.data.loader;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.TimeZone;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证规范行序列化能够区分边界值并统一常见逻辑值。
 */
class CanonicalRowSerializerTest {

    @Test
    void shouldDistinguishNullEmptyAndAmbiguousText() {
        StructType schema = new StructType()
                .add("first", DataTypes.StringType, true)
                .add("second", DataTypes.StringType, true);
        CanonicalRowSerializer serializer = new CanonicalRowSerializer(schema,
                RowIdentityConfig.NORMALIZATION_VERSION);

        String nullValue = serializer.serialize(RowFactory.create(null, "a|b"));
        String emptyValue = serializer.serialize(RowFactory.create("", "a|b"));
        String firstJoin = serializer.serialize(RowFactory.create("a|b", "c"));
        String secondJoin = serializer.serialize(RowFactory.create("a", "b|c"));

        assertNotEquals(nullValue, emptyValue);
        assertNotEquals(firstJoin, secondJoin);
        assertTrue(firstJoin.contains("a|b"));
    }

    @Test
    void shouldNormalizeDecimalDateTimestampBooleanAndUnicode() {
        StructType schema = new StructType()
                .add("amount", DataTypes.createDecimalType(20, 4), false)
                .add("day", DataTypes.DateType, false)
                .add("time", DataTypes.TimestampType, false)
                .add("enabled", DataTypes.BooleanType, false)
                .add("text", DataTypes.StringType, false);
        CanonicalRowSerializer serializer = new CanonicalRowSerializer(schema,
                RowIdentityConfig.NORMALIZATION_VERSION);
        Timestamp timestamp = Timestamp.valueOf("2026-07-19 12:34:56.123456789");

        String first = serializer.serialize(RowFactory.create(
                new BigDecimal("10.5000"), Date.valueOf("2026-07-19"),
                timestamp, true, "中文"));
        String second = serializer.serialize(RowFactory.create(
                new BigDecimal("10.5"), Date.valueOf("2026-07-19"),
                timestamp, true, "中文"));

        assertEquals(first, second);
    }

    @Test
    void shouldKeepTimestampStableAcrossDefaultTimeZones() {
        StructType schema = new StructType()
                .add("time", DataTypes.TimestampType, false);
        CanonicalRowSerializer serializer = new CanonicalRowSerializer(schema,
                RowIdentityConfig.NORMALIZATION_VERSION);
        Timestamp timestamp = Timestamp.from(Instant.parse(
                "2026-07-19T04:34:56.123456Z"));
        TimeZone original = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            String shanghai = serializer.serialize(RowFactory.create(timestamp));
            TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
            String losAngeles = serializer.serialize(RowFactory.create(timestamp));

            assertEquals(shanghai, losAngeles);
        } finally {
            TimeZone.setDefault(original);
        }
    }
}
