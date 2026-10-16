#!/usr/bin/env bash
# Stage 2 - GraphX analytics: clean Parquet in HDFS -> graph metrics + summaries in HDFS.
source "$(dirname "$0")/../common/common.sh"
require_hdfs
URI="$(hdfs_uri)"
hdfs dfs -test -e "$HDFS_CLEAN/edges/_SUCCESS" || die "No clean edges in HDFS. Run 1-etl/run_etl.sh first."

log "Stage 2: GraphX   $URI$HDFS_CLEAN  ->  $URI$HDFS_GRAPH"
spark_submit CitationGraphAnalytics \
  --clean "$URI$HDFS_CLEAN" --out "$URI$HDFS_GRAPH" \
  --pagerank-tol "$PAGERANK_TOL" --pagerank-reset "$PAGERANK_RESET" \
  --lpa-steps "$LPA_MAX_STEPS" --scc-iters "${SCC_ITERS:-10}" --landmarks "$LANDMARKS"

hdfs dfs -rm -r -f -skipTrash "$HDFS_GRAPH/_checkpoints" >/dev/null
ok "Output in HDFS:"
hdfs dfs -du "$HDFS_GRAPH" | awk '{printf "     %8.1f MB  %s\n", $1 / 1048576, $NF}'
