ADD JAR /opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETCOLLECT
AS 'com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT';

SELECT F_DW_DETCOLLECT(
    '{"sourceType":"SQL","datasetId":"person_info","sqlText":"select * from dw.person_info limit 400","labelingBudget":"300","samplingRound":"4"}'
);