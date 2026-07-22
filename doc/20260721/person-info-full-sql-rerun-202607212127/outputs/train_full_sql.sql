ADD JAR /opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETTRAIN
AS 'com.fiberhome.ml.raha.udf.F_DW_DETTRAIN';

SELECT F_DW_DETTRAIN(
    '{"sampleBatchId":"sample_dw.person_info@20260721134516.455","sourceType":"SQL","datasetId":"person_info","sqlText":"select * from dw.person_info limit 400","allowPartialAnnotation":"false","modelNamePrefix":"person_info_full_sql","publishZip":"true","artifactBaseDir":"/opt/spark/work-dir/data/person-info-full-sql-rerun-202607212127/udf-work","localWebRoot":"/opt/software/fmdb/manager/web/jetty/webapps/fmdb_report","hdfsExportPath":"/fmdb/detection/output/person-info-full-sql-rerun-202607212127/","webBaseUrl":"http://localhost:10030/fmdb_report","caller":"codex","requestId":"person-info-full-train-202607212127"}'
);