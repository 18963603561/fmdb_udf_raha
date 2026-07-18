package com.fiberhome.ml.raha.support;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 统一生成 ORC 分区日期。
 */
public final class TimeUtils {

    private TimeUtils() {
    }

    public static String partitionDate(long epochMillis, String zoneId) {
        return DateTimeFormatter.ISO_LOCAL_DATE.format(
                Instant.ofEpochMilli(epochMillis).atZone(ZoneId.of(zoneId)));
    }
}
