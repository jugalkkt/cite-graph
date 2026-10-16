#!/usr/bin/env bash
# Runs the whole Academic Citation Network Analyzer end to end on ogbn-mag:
#   HDFS (ingest) -> Spark (ETL) -> GraphX (analytics) -> results/ (tables + charts)
# Each step is idempotent, so re-running skips downloads and existing HDFS files.
set -euo pipefail
cd "$(dirname "$0")"
start=$(date +%s)

./common/setup_tools.sh        # Spark into ./tools
./0-ingest/download_data.sh    # ogbn-mag into ./data/raw
./common/start_hadoop.sh       # HDFS up
./0-ingest/ingest_hdfs.sh      # stage 0: raw files into HDFS
./common/build.sh              # Scala code into target/citation-analyzer.jar
./1-etl/run_etl.sh             # stage 1: Spark ETL
./2-graph/run_graph.sh         # stage 2: GraphX analytics
./3-export/export_results.sh   # stage 3: tables and charts into results/

source common/common.sh    # for RESULTS_DIR, which conf/local.env may override
printf '\n\033[1;32mPipeline finished in %d min %d s. Results: %s\033[0m\n' \
  $(( ($(date +%s) - start) / 60 )) $(( ($(date +%s) - start) % 60 )) "${RESULTS_DIR#"$PROJECT_ROOT"/}"
