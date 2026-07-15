package com.fiberhome.ml.raha.data.loader;

/**
 * 开发期文件加载器支持的数据格式。
 */
public enum DataFormat {
    /** 逗号分隔或自定义分隔文本。 */
    CSV("csv"),
    /** JSON 文本。 */
    JSON("json"),
    /** Parquet 列式文件。 */
    PARQUET("parquet");

    /** Spark 数据源格式名称。 */
    private final String sparkFormat;

    DataFormat(String sparkFormat) {
        this.sparkFormat = sparkFormat;
    }

    public String getSparkFormat() {
        return sparkFormat;
    }
}

