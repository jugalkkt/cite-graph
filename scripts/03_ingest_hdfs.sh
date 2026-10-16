#!/usr/bin/env bash
# Stage 0 - ingestion: streams the raw files into HDFS.
# Files are decompressed on the way in (gunzip | hdfs dfs -put -) because gzip is not
# splittable: a plain-text file can be read by several Spark tasks in parallel, one per block.
# Set FORCE=1 to overwrite files already in HDFS.
source "$(dirname "$0")/common.sh"
require_hdfs

[[ -d "$RAW_DIR/mag" ]] || die "Raw data missing. Run scripts/01_download_data.sh first."
log "Ingesting raw files into $(hdfs_uri)$HDFS_RAW"

# put <decompress command> <local file> <HDFS path under raw/>
put() {
  local unpack="$1" src="$2" dst="$HDFS_RAW/$3"
  [[ -f "$src" ]] || die "Missing $src"
  if hdfs dfs -test -e "$dst" && [[ "${FORCE:-0}" != 1 ]]; then
    ok "exists, skipped: $3"; return
  fi
  hdfs dfs -mkdir -p "$(dirname "$dst")"
  $unpack "$src" | hdfs dfs -put -f - "$dst"
  ok "$3"
}
put_gz() { put "gunzip -c" "$@"; }

# ogbn-mag is a heterogeneous graph; only its paper nodes and paper-cites-paper edges are used.
put_gz "$RAW_DIR/mag/raw/relations/paper___cites___paper/edge.csv.gz"  edges/edge.csv
put_gz "$RAW_DIR/mag/raw/node-feat/paper/node_year.csv.gz"             nodes/node_year.csv
put_gz "$RAW_DIR/mag/raw/node-label/paper/node-label.csv.gz"           nodes/node_label.csv
put_gz "$RAW_DIR/mag/mapping/paper_entidx2name.csv.gz"                 mapping/nodeidx2paperid.csv
put_gz "$RAW_DIR/mag/mapping/labelidx2venuename.csv.gz"                mapping/labelidx2venue.csv

log "HDFS contents"
hdfs dfs -ls -R "$HDFS_RAW" | grep -v '^d' | awk '{printf "     %8.1f MB  %s\n", $5 / 1048576, $8}'
hdfs dfs -du -s -h "$HDFS_RAW" | awk '{print "     total: " $1 " " $2}'
hdfs fsck "$HDFS_RAW" 2>/dev/null | grep -E "Total blocks|Status" | sed 's/^\s*/     /'
