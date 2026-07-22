#!/bin/sh
set -u
hdfs dfs -ls /user/hive/warehouse/dw.db/raha_* 2>/dev/null | awk '{print $8}' | sort -u > /tmp/raha_hdfs_paths.txt
cat /tmp/raha_hdfs_paths.txt
while IFS= read -r p; do
  if [ -n "$p" ]; then
    hdfs dfs -rm -r -skipTrash "$p"
  fi
done < /tmp/raha_hdfs_paths.txt