package com.fiberhome.ml.raha.data.domain;

import com.fiberhome.ml.raha.util.HashUtils;

/**
 * 保存检测时的单元格原始值、哈希和可选脱敏值。
 */
public final class CellValue {

    /** 单元格坐标。 */
    private final CellCoordinate coordinate;
    /** 输入快照中的原始值，允许为空。 */
    private final String rawValue;
    /** 原始值的稳定哈希。 */
    private final String valueHash;
    /** 可向日志或普通结果展示的脱敏值。 */
    private final String maskedValue;

    public CellValue(CellCoordinate coordinate, String rawValue, String valueHash, String maskedValue) {
        if (coordinate == null) {
            throw new IllegalArgumentException("单元格坐标不能为空");
        }
        if (valueHash == null || valueHash.trim().isEmpty()) {
            throw new IllegalArgumentException("单元格值哈希不能为空");
        }
        this.coordinate = coordinate;
        this.rawValue = rawValue;
        this.valueHash = valueHash;
        this.maskedValue = maskedValue;
    }

    /**
     * 根据原始值创建单元格值对象。
     *
     * @param coordinate 单元格坐标
     * @param rawValue 原始值
     * @param maskedValue 脱敏值
     * @return 单元格值对象
     */
    public static CellValue of(CellCoordinate coordinate, String rawValue, String maskedValue) {
        String hashSource = rawValue == null ? "<null>" : rawValue;
        return new CellValue(coordinate, rawValue, HashUtils.md5Hex(hashSource), maskedValue);
    }

    public CellCoordinate getCoordinate() {
        return coordinate;
    }

    public String getRawValue() {
        return rawValue;
    }

    public String getValueHash() {
        return valueHash;
    }

    public String getMaskedValue() {
        return maskedValue;
    }
}

