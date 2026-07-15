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
    PARQUET("parquet", true),
    /** FMDB Catalog 中的表。 */
    FMDB_TABLE("fmdb-table", false),
    /** FMDB 会话执行的只读 SQL。 */
    FMDB_SQL("fmdb-sql", false);

    /** Spark 数据源格式名称。 */
    private final String sparkFormat;
    /** 是否可以由开发期文件加载器读取。 */
    private final boolean fileFormat;

    DataFormat(String sparkFormat) {
        this(sparkFormat, true);
    }

    DataFormat(String sparkFormat, boolean fileFormat) {
        this.sparkFormat = sparkFormat;
        this.fileFormat = fileFormat;
    }

    public String getSparkFormat() {
        return sparkFormat;
    }

    public boolean isFileFormat() {
        return fileFormat;
    }
}
