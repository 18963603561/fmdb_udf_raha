package com.fiberhome.ml.raha.fmdb;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * 九张最终物理表共用的严格字段记录对象。
 */
public final class FmdbTableRecord {

    /** 记录所属物理表。 */
    private final FmdbPhysicalTable table;
    /** 按物理字段名保存的不可变值。 */
    private final Map<String, Object> values;

    private FmdbTableRecord(FmdbPhysicalTable table, Map<String, Object> values) {
        if (table == null || values == null) {
            throw new IllegalArgumentException("FMDB 记录表和字段不能为空");
        }
        // 在构造阶段校验字段完整性，避免写入器生成半结构化错误行。
        FmdbTableSchemas.row(table, values);
        this.table = table;
        this.values = Collections.unmodifiableMap(
                new LinkedHashMap<String, Object>(values));
    }

    /**
     * 创建标准表记录。
     *
     * @param table 物理表
     * @param values 字段值
     * @return 严格校验后的记录
     */
    public static FmdbTableRecord of(FmdbPhysicalTable table,
                                     Map<String, Object> values) {
        return new FmdbTableRecord(table, values);
    }

    public FmdbPhysicalTable getTable() {
        return table;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Row toRow() {
        return FmdbTableSchemas.row(table, values);
    }
}
