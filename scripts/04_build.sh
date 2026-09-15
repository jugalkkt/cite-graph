#!/usr/bin/env bash
# Compiles the Scala/Spark application into spark-app/target/citation-analyzer.jar.
source "$(dirname "$0")/common.sh"

log "Building the Spark application with scala-cli"
mkdir -p "$(dirname "$APP_JAR")"
# --server=false: compile in-process instead of leaving a ~500 MB Bloop daemon running afterwards
scala-cli --power package "$PROJECT_ROOT/spark-app" --server=false --library --force -o "$APP_JAR"
ok "Built $APP_JAR ($(du -h "$APP_JAR" | cut -f1))"
