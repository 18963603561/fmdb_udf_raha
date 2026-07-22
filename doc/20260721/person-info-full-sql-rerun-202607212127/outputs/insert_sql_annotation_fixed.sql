INSERT OVERWRITE TABLE dw.raha_annotation_record
PARTITION (dataset_id='fmdb-table:dw.person_info', partition_month='2026-07')
SELECT
  'ann_sample_dw.person_info_20260721134516.455_sql' AS annotation_batch_id,
  sample_batch_id,
  concat('task-', row_id) AS annotation_task_id,
  row_id,
  row_content_hash,
  row_data_json,
  'sql-auto-label-v1' AS template_version,
  'sql-auto-label-sample_dw.person_info_20260721134516.455.xls' AS file_name,
  schema_hash,
  to_json(named_struct(
    'cellLabels', named_struct(
      'name', CAST(name_error AS INT),
      'age', CAST(age_error AS INT),
      'phone', CAST(phone_error AS INT),
      'id_card', CAST(id_error AS INT),
      'email', CAST(email_error AS INT),
      'home_address', CAST(address_error AS INT)
    ),
    'comment', 'sql-auto-label',
    'errorColumns', array('name','age','phone','id_card','email','home_address'),
    'importFingerprint', 'sql-auto-label-sample_dw.person_info_20260721134516.455',
    'reviewedColumns', array('name','age','phone','id_card','email','home_address'),
    'rowLabel', IF(name_error + age_error + phone_error + id_error + email_error + address_error > 0, 1, 0),
    'sourceSnapshotId', 'snapshot_dw.person_info@content-d6f8fc08216579c6a5e5a9df6ce7617a'
  )) AS annotation_json,
  'sql-auto' AS annotator,
  'IMPORTED' AS batch_status,
  COUNT(*) OVER () AS batch_record_count,
  COUNT(*) OVER () AS valid_record_count,
  CAST(0 AS BIGINT) AS invalid_record_count,
  CAST(NULL AS STRING) AS supersedes_batch_id,
  CAST(1784641700000 AS BIGINT) AS annotated_at
FROM (
  SELECT
    sample_batch_id,
    row_id,
    row_content_hash,
    row_data_json,
    schema_hash,
    IF(length(trim(get_json_object(row_data_json, '$.name'))) = 0, 1, 0) AS name_error,
    IF(CAST(get_json_object(row_data_json, '$.age') AS INT) < 18
       OR CAST(get_json_object(row_data_json, '$.age') AS INT) > 90, 1, 0) AS age_error,
    IF(NOT (get_json_object(row_data_json, '$.phone') RLIKE '^1[3-9][0-9]{9}$'), 1, 0) AS phone_error,
    IF(NOT (get_json_object(row_data_json, '$.id_card') RLIKE '^[0-9]{17}[0-9Xx]$'), 1, 0) AS id_error,
    IF(NOT (get_json_object(row_data_json, '$.email') RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][A-Za-z]{2,}$'), 1, 0) AS email_error,
    IF(length(trim(get_json_object(row_data_json, '$.home_address'))) < 6, 1, 0) AS address_error
  FROM dw.raha_sample_record
  WHERE sample_batch_id='sample_dw.person_info@20260721134516.455'
) s;

SELECT batch_status, count(*) AS cnt, min(valid_record_count) AS valid_cnt
FROM dw.raha_annotation_record
WHERE sample_batch_id='sample_dw.person_info@20260721134516.455'
GROUP BY batch_status;