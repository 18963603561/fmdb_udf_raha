-- Jar 必须位于 Spark 驱动进程可读取的位置。
ADD JAR /opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

-- 三个入口均要求驱动进程单次调用，不能按输入数据行执行。
CREATE TEMPORARY FUNCTION F_DW_RAHASAMPLE
AS 'com.fiberhome.ml.raha.udf.F_DW_RAHASAMPLE';

CREATE TEMPORARY FUNCTION F_DW_RAHATRAIN
AS 'com.fiberhome.ml.raha.udf.F_DW_RAHATRAIN';

CREATE TEMPORARY FUNCTION F_DW_RAHADETECT
AS 'com.fiberhome.ml.raha.udf.F_DW_RAHADETECT';
