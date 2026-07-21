ADD JAR /opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETTRAIN
AS 'com.fiberhome.ml.raha.udf.F_DW_DETTRAIN';

SELECT inline(F_DW_DETTRAIN(
    '{"sampleBatchId":"sample-15d1d5b6681c79f83f4adef1","sourceType":"SQL","datasetId":"person_info","sqlText":"select * from dw.person_info","annotationDir":"/fmdb/detection/annotation/","allowPartialAnnotation":"false","modelNamePrefix":"person_info_r6","publishZip":"true","artifactBaseDir":"/opt/spark/work-dir/data/person-info-raha-run-202607201851/udf-work","localWebRoot":"/opt/spark/work-dir/data/person-info-raha-run-202607201851/udf-web","hdfsExportPath":"/fmdb/detection/output/person-info-raha-run-202607201851/","caller":"codex","requestId":"person-info-train-202607201851-r6"}'
));
