package com.fiberhome.ml.raha.repository.adapter.fmdb.support;

import com.fiberhome.ml.raha.data.domain.ColumnProfile;
import java.util.Map;

/**
 * 定义训练列画像在列级产物表中的稳定 JSON 往返协议。
 */
public final class FmdbColumnProfileCodec {

    private FmdbColumnProfileCodec() {
    }

    public static String write(ColumnProfile profile) {
        if (profile == null) {
            throw new IllegalArgumentException("列画像不能为空");
        }
        return FmdbJsonCodec.write(profile);
    }

    public static ColumnProfile read(String json) {
        Map<String, Object> values = FmdbJsonCodec.readObject(json);
        return new ColumnProfile(
                FmdbJsonValue.requiredText(values, "columnName"),
                FmdbJsonValue.requiredNumber(values, "totalCount").longValue(),
                FmdbJsonValue.requiredNumber(values, "nullCount").longValue(),
                FmdbJsonValue.requiredNumber(values, "blankCount").longValue(),
                FmdbJsonValue.requiredNumber(values, "distinctCount").longValue(),
                FmdbJsonValue.requiredNumber(values, "minLength").intValue(),
                FmdbJsonValue.requiredNumber(values, "maxLength").intValue(),
                FmdbJsonValue.requiredNumber(values, "averageLength").doubleValue(),
                FmdbJsonValue.requiredNumber(values, "numericCount").longValue(),
                FmdbJsonValue.requiredNumber(values, "numericRatio").doubleValue(),
                doubleValue(values, "numericMin"), doubleValue(values, "numericMax"),
                doubleValue(values, "numericMean"),
                doubleValue(values, "numericStandardDeviation"),
                doubleValue(values, "numericQ1"), doubleValue(values, "numericMedian"),
                doubleValue(values, "numericQ3"),
                FmdbJsonValue.longMap(values, "typeCounts"),
                FmdbJsonValue.longMap(values, "valueHashFrequencies"));
    }

    private static Double doubleValue(Map<String, Object> values, String key) {
        Number value = FmdbJsonValue.optionalNumber(values, key);
        return value == null ? null : value.doubleValue();
    }
}
