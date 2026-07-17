-- 将路径替换为 Driver 可读取的实际 Shade Jar 路径。
ADD JAR /path/to/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

-- 三个函数互不依赖，可按需单独执行任意一条 CREATE 语句。
CREATE TEMPORARY FUNCTION F_DW_RAHATRAIN
AS 'com.fiberhome.ml.raha.udf.F_DW_RAHATRAIN';

CREATE TEMPORARY FUNCTION F_DW_RAHADETECT
AS 'com.fiberhome.ml.raha.udf.F_DW_RAHADETECT';

CREATE TEMPORARY FUNCTION F_DW_RAHASAMPLE
AS 'com.fiberhome.ml.raha.udf.F_DW_RAHASAMPLE';
