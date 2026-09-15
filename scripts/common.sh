#!/usr/bin/env bash
# Shared helpers for the pipeline scripts. Source this file, don't execute it.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../conf/pipeline.env
source "$PROJECT_ROOT/conf/pipeline.env"

export JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR SPARK_HOME
export PATH="$JAVA_HOME/bin:$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
# Bind Spark to loopback so its executor host matches the single-node HDFS DataNode. Otherwise tasks
# that prefer HDFS-local blocks (e.g. checkpoints) wait for a host that has no executor.
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
# start-dfs.sh refuses to run without these when launched outside the course shell
export HDFS_NAMENODE_USER="$USER" HDFS_DATANODE_USER="$USER" HDFS_SECONDARYNAMENODE_USER="$USER"

log()  { printf '\n\033[1;34m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  ✔ %s\033[0m\n' "$*"; }
die()  { printf '\033[1;31m  ✘ %s\033[0m\n' "$*" >&2; exit 1; }

# hdfs://host:port taken from core-site.xml, so paths below are fully qualified
hdfs_uri() { hdfs getconf -confKey fs.defaultFS 2>/dev/null; }

require_hdfs() {
  hdfs dfs -test -d / 2>/dev/null || die "HDFS is not reachable. Run scripts/02_start_hadoop.sh first."
}

# "2g" -> 2048, "1536m" -> 1536
to_mb() { local v="${1,,}"; case "$v" in *g) echo $(( ${v%g} * 1024 ));; *m) echo "${v%m}";; *) echo "$v";; esac; }

# Runs spark-submit with two memory safeguards:
#  1. refuses to start when the machine does not have enough free RAM for the job;
#  2. runs Spark inside a systemd cgroup capped at MEMORY_CAP_MB, so a runaway job is killed
#     on its own instead of the kernel OOM killer picking other applications.
spark_submit() {
  local class="$1"; shift
  [[ -f "$APP_JAR" ]] || die "Missing $APP_JAR. Run scripts/04_build.sh first."
  local avail_mb; avail_mb="$(awk '/MemAvailable/ {print int($2 / 1024)}' /proc/meminfo)"
  (( avail_mb >= MEMORY_CAP_MB )) || die "Only ${avail_mb} MB RAM available but Spark may use up to ${MEMORY_CAP_MB} MB. \
Close some applications, or lower DRIVER_MEMORY and MEMORY_CAP_MB."
  (( MEMORY_CAP_MB > $(to_mb "$DRIVER_MEMORY") + 768 )) || die "MEMORY_CAP_MB must exceed DRIVER_MEMORY by at least 768 MB of JVM overhead"
  local cap=()
  if command -v systemd-run >/dev/null && systemd-run --user --scope -q true 2>/dev/null; then
    cap=(systemd-run --user --scope -q -p "MemoryMax=${MEMORY_CAP_MB}M" -p MemorySwapMax=0)
  else
    printf '  (systemd-run unavailable: running Spark without a memory cgroup)\n' >&2
  fi
  mkdir -p "$SPARK_SCRATCH"
  # MALLOC_ARENA_MAX: glibc otherwise creates one malloc arena per thread, inflating JVM native memory.
  local status=0
  MALLOC_ARENA_MAX=2 "${cap[@]}" spark-submit \
    --class "edu.cse412.citation.$class" \
    --master "$SPARK_MASTER" \
    --driver-memory "$DRIVER_MEMORY" \
    --conf spark.sql.shuffle.partitions="$SHUFFLE_PARTITIONS" \
    --conf spark.ui.showConsoleProgress=false \
    --conf spark.locality.wait=0s \
    --conf spark.local.dir="$SPARK_SCRATCH" \
    --conf spark.driver.extraJavaOptions="-XX:MaxDirectMemorySize=512m -XX:ReservedCodeCacheSize=128m -Djava.io.tmpdir=$SPARK_SCRATCH -Dlog4j2.configurationFile=file:$PROJECT_ROOT/conf/log4j2.properties" \
    "$APP_JAR" "$@" || status=$?
  rm -rf "${SPARK_SCRATCH:?}"/*   # shuffle files are only needed while the job runs
  return $status
}
