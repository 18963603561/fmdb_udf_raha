ADD JAR /opt/spark/work-dir/data/person-info-500-full-20260723135436/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar;

CREATE TEMPORARY FUNCTION F_DW_DETCOLLECT
AS 'com.fiberhome.ml.raha.udf.F_DW_DETCOLLECT';

SELECT F_DW_DETCOLLECT(
    '{"sourceType":"SQL","datasetId":"person_info","sqlText":"select * from dw.person_info","sourceVersion":"person-info-docker-500-full-20260723135436","labelingBudget":"500","samplingRound":"6","autoLabelEnabled":"true","autoLabelModelUrl":"https://api.deepseek.com/v1/chat/completions","autoLabelApiKeyEnv":"RAHA_AUTO_LABEL_API_KEY","autoLabelModel":"deepseek-v4-flash","autoLabelMaxRowsPerBatch":"10","autoLabelMaxColumnsPerBatch":"6","autoLabelMaxCharsPerBatch":"30000","autoLabelMaxOutputTokens":"8192","autoLabelMaxParallelBatches":"1","autoLabelMaxRetryCount":"2","autoLabelBatchTimeoutMillis":"240000","autoLabelFailPolicy":"FAIL","autoLabelMaskSensitiveColumns":"false","publishZip":"false","artifactBaseDir":"/opt/spark/work-dir/data/person-info-500-full-20260723135436/udf-work","localWebRoot":"/opt/spark/work-dir/data/person-info-500-full-20260723135436/web","caller":"codex","requestId":"person-info-collect-docker-500-full-20260723135436","forceRun":true,"forceRunId":"c-docker-500-full-20260723135436"}'
);
