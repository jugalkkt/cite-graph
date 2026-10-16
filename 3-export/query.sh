#!/usr/bin/env bash
# Runs one Spark SQL query against the pipeline's results in HDFS.
#   ./3-export/query.sh "SELECT title, pagerank_rank FROM papers WHERE category = 'venue:1980519' ORDER BY pagerank_rank LIMIT 10"
# Views: papers, communities, citations (see QueryResults.scala for columns).
source "$(dirname "$0")/../common/common.sh"
require_hdfs
[[ $# -ge 1 ]] || die "usage: $0 \"<SQL>\" [rows]"
URI="$(hdfs_uri)"
spark_submit QueryResults --graph "$URI$HDFS_GRAPH" --clean "$URI$HDFS_CLEAN" --sql "$1" --rows "${2:-25}"
