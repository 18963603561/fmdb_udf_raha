package com.fiberhome.ml.raha.fmdb;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 统一生成 FMDB 分区字段，固定使用 UTC 避免跨时区产生不同目录。
 */
public final class FmdbPartitionUtils {

    /** 月分区格式。 */
    private static final DateTimeFormatter MONTH =
            DateTimeFormatter.ofPattern("yyyy-MM").withZone(ZoneOffset.UTC);
    /** 日分区格式。 */
    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    private FmdbPartitionUtils() {
    }

    public static String month(long epochMillis) {
        requireTime(epochMillis);
        return MONTH.format(Instant.ofEpochMilli(epochMillis));
    }

    public static String date(long epochMillis) {
        requireTime(epochMillis);
        return DATE.format(Instant.ofEpochMilli(epochMillis));
    }

    private static void requireTime(long epochMillis) {
        if (epochMillis <= 0L) {
            throw new IllegalArgumentException("FMDB 分区时间必须大于 0");
        }
    }
}
