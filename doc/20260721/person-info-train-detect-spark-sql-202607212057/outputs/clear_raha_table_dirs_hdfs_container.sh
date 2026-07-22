#!/bin/sh
set -u
HDFS=/opt/hadoop/bin/hdfs
for name in raha_sample_batch raha_sample_tuple raha_cell_label raha_model_set raha_column_model raha_detection_batch \
  raha_toy_dirty_20260720132259 raha_toy_dirty_20260720132923 raha_toy_dirty_20260720133140 \
  raha_toy_dirty_20260720133733 raha_toy_dirty_20260720134230 raha_toy_dirty_20260720134955 \
  raha_toy_dirty_20260720135531 raha_toy_dirty_20260720140808 raha_toy_dirty_20260720143556 \
  raha_toy_dirty_20260720144258 raha_sample_record raha_annotation_record raha_training_column_artifact \
  raha_training_cell raha_training_example raha_model_artifact raha_detection_result raha_job_run raha_job_stage_attempt; do
  p=/user/hive/warehouse/dw.db/$name
  echo $p
  $HDFS dfs -rm -r -skipTrash $p 2>/dev/null || true
  $HDFS dfs -mkdir -p $p
  $HDFS dfs -chmod -R 777 $p
done