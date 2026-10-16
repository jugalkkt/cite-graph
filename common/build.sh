#!/usr/bin/env bash
# Compiles the Scala/Spark code of every stage (common/, 1-etl/, 2-graph/, 3-export/) into
# target/citation-analyzer.jar. Build settings are in common/project.scala.
source "$(dirname "$0")/common.sh"

log "Building the Spark application with scala-cli"
mkdir -p "$(dirname "$APP_JAR")"
# --server=false: compile in-process instead of leaving a ~500 MB Bloop daemon running afterwards
scala-cli --power package "$PROJECT_ROOT/common" "$PROJECT_ROOT/1-etl" "$PROJECT_ROOT/2-graph" "$PROJECT_ROOT/3-export" \
  --server=false --library --force -o "$APP_JAR"
ok "Built $APP_JAR ($(du -h "$APP_JAR" | cut -f1))"
