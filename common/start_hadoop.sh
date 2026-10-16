#!/usr/bin/env bash
# Starts HDFS (and YARN when SPARK_MASTER=yarn) and waits until the NameNode leaves safe mode.
source "$(dirname "$0")/common.sh"

log "Starting HDFS"
if jps | grep -qw NameNode; then
  ok "NameNode already running"
else
  start-dfs.sh
fi

if [[ "$SPARK_MASTER" == yarn* ]]; then
  if jps | grep -qw ResourceManager; then ok "YARN already running"; else start-yarn.sh; fi
fi

hdfs dfsadmin -safemode wait >/dev/null
hdfs dfs -mkdir -p "/user/$USER"
ok "HDFS is up at $(hdfs_uri)"
jps | grep -E "NameNode|DataNode|ResourceManager|NodeManager" | sed 's/^/     /'
