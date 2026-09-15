#!/usr/bin/env bash
# Copies the small summary tables out of HDFS into ./results and draws the report charts.
source "$(dirname "$0")/common.sh"
require_hdfs

log "Exporting summaries from HDFS to $RESULTS_DIR"
mkdir -p "$RESULTS_DIR/tables" "$RESULTS_DIR/charts"
hdfs dfs -getmerge "$HDFS_CLEAN/quality_report/part-*.csv" "$RESULTS_DIR/tables/quality_report.csv"
for dir in $(hdfs dfs -ls -C "$HDFS_GRAPH/summary"); do
  name="$(basename "$dir")"
  hdfs dfs -getmerge "$dir/part-*.csv" "$RESULTS_DIR/tables/$name.csv"
done
rm -f "$RESULTS_DIR"/tables/.*.crc
ok "$(ls "$RESULTS_DIR/tables" | wc -l) tables in results/tables"

log "Drawing charts"
python3 "$PROJECT_ROOT/analysis/make_charts.py" "$RESULTS_DIR/tables" "$RESULTS_DIR/charts"
