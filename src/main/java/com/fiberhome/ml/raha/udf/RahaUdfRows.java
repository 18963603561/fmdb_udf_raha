package com.fiberhome.ml.raha.udf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 保存 UDF 逻辑输出行，最终由 GenericUDF 转换成 Hive 标准结构。
 */
public final class RahaUdfRows {

    /** 按返回顺序保存的行数据。 */
    private final List<Map<String, Object>> rows;

    public RahaUdfRows(List<Map<String, Object>> rows) {
        if (rows == null) {
            throw new IllegalArgumentException("UDF 输出行不能为空");
        }
        List<Map<String, Object>> copy =
                new ArrayList<Map<String, Object>>(rows.size());
        for (Map<String, Object> row : rows) {
            if (row == null) {
                throw new IllegalArgumentException("UDF 输出行不能包含空行");
            }
            copy.add(Collections.unmodifiableMap(
                    new LinkedHashMap<String, Object>(row)));
        }
        this.rows = Collections.unmodifiableList(copy);
    }

    public static RahaUdfRows single(Map<String, Object> row) {
        List<Map<String, Object>> rows =
                new ArrayList<Map<String, Object>>(1);
        rows.add(row);
        return new RahaUdfRows(rows);
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }
}
