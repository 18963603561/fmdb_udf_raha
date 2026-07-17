#!/bin/sh
set -eu

output_dir="/opt/spark/work-dir/data/raha-movies1-validation-20260716-1615"
mkdir -p "${output_dir}"

exec /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  --deploy-mode client \
  --driver-memory 2800m \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.eventLog.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=2 \
  --conf spark.driver.maxResultSize=1g \
  --conf "spark.driver.extraJavaOptions=-Dfmdb.validation.mode=COMBINED -Dfmdb.validation.dataset-id=movies_1 -Dfmdb.validation.snapshot-id=movies-1-snapshot-v4 -Dfmdb.validation.row-id-column=tuple_id -Dfmdb.validation.worker-timeout-millis=3600000 -Draha.job.random-seed=20260715 -Draha.resource.max-parallel-strategies=2 -Draha.resource.max-parallel-columns=2" \
  --class com.fiberhome.ml.raha.app.RahaContainerValidationApplication \
  /opt/spark/work-dir/target/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar \
  /opt/spark/work-dir/data/raha-movies-1-indexed/dirty.csv \
  /opt/spark/work-dir/data/raha-movies-1-indexed/clean.csv \
  "${output_dir}" > "${output_dir}/java.log" 2>&1
