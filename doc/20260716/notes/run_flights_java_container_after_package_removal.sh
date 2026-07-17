#!/bin/sh
set -eu

output_dir="${1:-/opt/spark/work-dir/data/raha-flights-validation-package-removal-20260716-1830}"
mkdir -p "${output_dir}"

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1800m \
  --executor-memory 1g \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=1 \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.eventLog.enabled=false \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=2 \
  --conf spark.driver.maxResultSize=512m \
  --conf "spark.driver.extraJavaOptions=-Dfmdb.validation.mode=COMBINED -Dfmdb.validation.dataset-id=flights -Dfmdb.validation.snapshot-id=flights-package-removal-v1 -Dfmdb.validation.row-id-column=tuple_id -Dfmdb.validation.worker-timeout-millis=1800000 -Draha.job.random-seed=20260715 -Draha.resource.max-parallel-strategies=2 -Draha.resource.max-parallel-columns=2" \
  --class com.fiberhome.ml.raha.app.RahaContainerValidationApplication \
  /opt/spark/work-dir/target/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar \
  /opt/spark/work-dir/data/raha-flights-package-removal/dirty.csv \
  /opt/spark/work-dir/data/raha-flights-package-removal/clean.csv \
  "${output_dir}" > "${output_dir}/java.log" 2>&1
