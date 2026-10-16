#!/usr/bin/env bash
# Stage 1 - Spark ETL: HDFS raw text -> clean Parquet vertices/edges in HDFS.
source "$(dirname "$0")/../common/common.sh"
require_hdfs
URI="$(hdfs_uri)"

log "Stage 1: Spark ETL   $URI$HDFS_RAW  ->  $URI$HDFS_CLEAN"
spark_submit CitationETL --raw "$URI$HDFS_RAW" --out "$URI$HDFS_CLEAN"

ok "Output in HDFS:"
hdfs dfs -du "$HDFS_CLEAN" | awk '{printf "     %8.1f MB  %s\n", $1 / 1048576, $NF}'
