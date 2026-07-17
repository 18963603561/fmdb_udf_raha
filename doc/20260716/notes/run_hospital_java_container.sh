#!/bin/sh
set -eu

output_dir="${1:-/opt/spark/work-dir/data/raha-hospital-comparison-20260716/output}"
strategy_families="${2:-OD,PVD,RVD}"
mkdir -p "${output_dir}"

exec /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1800m \
  --executor-memory 1g \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=2 \
  --conf spark.cores.max=2 \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.eventLog.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.default.parallelism=2 \
  --conf spark.driver.maxResultSize=512m \
  --conf "spark.driver.extraJavaOptions=-Dfmdb.validation.mode=COMBINED -Dfmdb.validation.dataset-id=hospital -Dfmdb.validation.snapshot-id=hospital-java-python-20260716-v1 -Dfmdb.validation.row-id-column=index -Dfmdb.validation.worker-timeout-millis=3600000 -Draha.job.random-seed=20260715 -Draha.strategy.families=${strategy_families} -Draha.resource.max-parallel-strategies=2 -Draha.resource.max-parallel-columns=2 -Draha.resource.stage-timeout-millis=3600000" \
  --class com.fiberhome.ml.raha.app.RahaContainerValidationApplication \
  /opt/spark/work-dir/target/fmdb-udf-raha-1.0.0-SNAPSHOT-all.jar \
  /opt/spark/work-dir/data/raha-hospital-comparison-20260716/dirty.csv \
  /opt/spark/work-dir/data/raha-hospital-comparison-20260716/clean.csv \
  "${output_dir}" > "${output_dir}/java.log" 2>&1
