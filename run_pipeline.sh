#!/usr/bin/env bash
# Runs the whole Academic Citation Network Analyzer end to end on ogbn-mag (DATASET=arxiv for ogbn-arxiv):
#   HDFS (ingest) -> Spark (ETL) -> GraphX (analytics) -> results/ (tables + charts)
# Each step is idempotent, so re-running skips downloads and existing HDFS files.
set -euo pipefail
cd "$(dirname "$0")"
start=$(date +%s)

./scripts/00_setup_tools.sh
./scripts/01_download_data.sh
./scripts/02_start_hadoop.sh
./scripts/03_ingest_hdfs.sh
./scripts/04_build.sh
./scripts/05_run_etl.sh
./scripts/06_run_graph.sh
./scripts/07_export_results.sh

source scripts/common.sh   # for RESULTS_DIR, which depends on DATASET and conf/local.env
printf '\n\033[1;32mPipeline finished in %d min %d s. Results: %s\033[0m\n' \
  $(( ($(date +%s) - start) / 60 )) $(( ($(date +%s) - start) % 60 )) "${RESULTS_DIR#"$PROJECT_ROOT"/}"
