ADD JAR /opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETCOLLECT
AS 'com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT';

SELECT F_DW_DETCOLLECT(
    '{"sourceType":"SQL","datasetId":"person_info","sqlText":"select * from dw.person_info limit 450","labelingBudget":"300","samplingRound":"4","publishZip":"true","artifactBaseDir":"/opt/spark/work-dir/data/person-info-raha-run-202607201851/udf-work","localWebRoot":"/opt/spark/work-dir/data/person-info-raha-run-202607201851/udf-web","hdfsExportPath":"/fmdb/detection/output/person-info-raha-run-202607201851/","caller":"codex","requestId":"person-info-collect-202607201851-r4"}'
);
