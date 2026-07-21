ADD JAR /opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETRUN
AS 'com.fiberhome.ml.raha.udf.F_DW_DETRUN';

SELECT inline(F_DW_DETRUN(
    '{"sourceType":"SQL","datasetId":"person_info","sqlText":"select * from dw.person_info","snapshotId":"snapshot-46aef144474ed44fe3dd01c8","sourceVersion":"person-info-detect-r6","modelSetVersion":"ded3f7875f3f62361945078dbc9c2b0f93a7270d65e077929122884d9181417e","missingModelPolicy":"PARTIAL","detailFormat":"xls","publishZip":"true","artifactBaseDir":"/opt/spark/work-dir/data/person-info-raha-run-202607201851/udf-work","localWebRoot":"/opt/spark/work-dir/data/person-info-raha-run-202607201851/udf-web","hdfsExportPath":"/fmdb/detection/output/person-info-raha-run-202607201851/","caller":"codex","requestId":"person-info-detect-202607201851-r6"}'
));
