#!/usr/bin/env bash
# Downloads the ogbn-arxiv citation graph and the paper titles into ./data/raw.
#
# snap.stanford.edu throttles every connection to ~6 KB/s, so a plain download of the
# 150 MB takes hours. We split each file into 1 MB HTTP range requests fetched in parallel
# (DOWNLOAD_PARALLELISM connections), then stitch and integrity-check the result.
source "$(dirname "$0")/common.sh"
mkdir -p "$RAW_DIR"
PARALLEL="${DOWNLOAD_PARALLELISM:-32}"
CHUNK=$((1024 * 1024))

fetch() {
  local url="$1" out="$2"
  if [[ -s "$out" ]]; then ok "Already downloaded: $(basename "$out")"; return; fi

  local size parts n
  size="$(curl -sIL "$url" | tr -d '\r' | awk 'tolower($1)=="content-length:" {v=$2} END {print v}')"
  [[ "$size" =~ ^[0-9]+$ ]] || die "Could not read the size of $url"
  n=$(( (size + CHUNK - 1) / CHUNK ))
  parts="$out.parts"; mkdir -p "$parts"
  log "Downloading $(basename "$out"): $((size / 1024 / 1024)) MB in $n chunks, $PARALLEL at a time"

  seq 0 $((n - 1)) | xargs -P "$PARALLEL" -I{} bash -c '
    i={}; f="'"$parts"'/$(printf %05d $i)"
    from=$((i * '"$CHUNK"')); to=$((from + '"$CHUNK"' - 1)); want=$((to < '"$size"' ? '"$CHUNK"' : '"$size"' - from))
    for try in 1 2 3 4 5; do
      [[ -f "$f" && $(stat -c %s "$f") -eq $want ]] && exit 0
      curl -s --max-time 900 -r "$from-$to" -o "$f" "'"$url"'"
    done
    [[ -f "$f" && $(stat -c %s "$f") -eq $want ]] || { echo "chunk $i failed" >&2; exit 1; }
  ' || die "Some chunks failed; re-run this script to resume"

  cat "$parts"/* > "$out.tmp"
  [[ "$(stat -c %s "$out.tmp")" -eq "$size" ]] || die "Size mismatch for $(basename "$out")"
  mv "$out.tmp" "$out" && rm -rf "$parts"
  ok "$(basename "$out") ($(du -h "$out" | cut -f1))"
}

fetch "$ARXIV_URL"  "$RAW_DIR/arxiv.zip"
fetch "$TITLES_URL" "$RAW_DIR/titleabs.tsv.gz"

log "Integrity checks"
unzip -tq "$RAW_DIR/arxiv.zip" >/dev/null && ok "arxiv.zip is a valid archive"
gzip -t "$RAW_DIR/titleabs.tsv.gz" && ok "titleabs.tsv.gz is a valid gzip"

if [[ ! -d "$RAW_DIR/arxiv" ]]; then
  unzip -q "$RAW_DIR/arxiv.zip" -d "$RAW_DIR"
fi
ok "Raw files:"
find "$RAW_DIR/arxiv" -name "*.csv.gz" -printf "     %-50P %8k KB\n" | sort
