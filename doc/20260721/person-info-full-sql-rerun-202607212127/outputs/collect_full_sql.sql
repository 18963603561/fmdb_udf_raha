ADD JAR /opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETCOLLECT
AS 'com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT';

SELECT F_DW_DETCOLLECT(
    '{"sourceType":"SQL","datasetId":"person_info","sqlText":"select * from dw.person_info limit 400","labelingBudget":"300","samplingRound":"4","publishZip":"true","artifactBaseDir":"/opt/spark/work-dir/data/person-info-full-sql-rerun-202607212127/udf-work","localWebRoot":"/opt/software/fmdb/manager/web/jetty/webapps/fmdb_report","hdfsExportPath":"/fmdb/detection/output/person-info-full-sql-rerun-202607212127/","webBaseUrl":"http://localhost:10030/fmdb_report","caller":"codex","requestId":"person-info-full-collect-202607212127"}'
);