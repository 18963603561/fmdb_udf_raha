#!/usr/bin/env sh
set -eu

JAR_PATH=${1:-/opt/spark/work-dir/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar}
DIRTY_PATH=${2:-/opt/spark/work-dir/data/raha-toy-lightweight-20260717/dirty.csv}
CLEAN_PATH=${3:-/opt/spark/work-dir/data/raha-toy-lightweight-20260717/clean.csv}
REPORT_PATH=${4:-/tmp/raha-toy-validation-report.json}

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class com.fiberhome.ml.raha.udf.RahaToyUdfValidationMain \
  --conf spark.driver.host=spark-client \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.exec.dynamic.partition=true \
  --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=4 \
  "$JAR_PATH" \
  "$DIRTY_PATH" \
  "$CLEAN_PATH" \
  "$REPORT_PATH"
