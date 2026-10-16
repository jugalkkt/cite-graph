#!/usr/bin/env bash
# Downloads Apache Spark into ./tools (no sudo needed) and checks the other prerequisites.
source "$(dirname "$0")/common.sh"

log "Checking prerequisites"
[[ -x "$JAVA_HOME/bin/java" ]] || die "Java 17+ not found at $JAVA_HOME (set CITATION_JAVA_HOME)"
ok "Java: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
[[ -x "$HADOOP_HOME/bin/hdfs" ]] || die "Hadoop not found at $HADOOP_HOME (set HADOOP_HOME)"
ok "Hadoop: $(hadoop version 2>/dev/null | head -1)"
command -v scala-cli >/dev/null || die "scala-cli not found: install it from https://scala-cli.virtuslab.org/install"
ok "scala-cli: $(scala-cli version 2>/dev/null | head -1)"

if [[ -x "$SPARK_HOME/bin/spark-submit" ]]; then
  ok "Spark already installed at $SPARK_HOME"
else
  log "Downloading $SPARK_PACKAGE"
  mkdir -p "$PROJECT_ROOT/tools"
  tgz="$PROJECT_ROOT/tools/$SPARK_PACKAGE.tgz"
  curl -fL --retry 3 -o "$tgz" "https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/$SPARK_PACKAGE.tgz" \
    || curl -fL --retry 3 -o "$tgz" "https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/$SPARK_PACKAGE.tgz"
  log "Verifying SHA-512 checksum"
  expected="$(curl -fsSL "https://downloads.apache.org/spark/spark-$SPARK_VERSION/$SPARK_PACKAGE.tgz.sha512" \
              | awk '{print $1}')"
  actual="$(sha512sum "$tgz" | awk '{print $1}')"
  [[ "$expected" == "$actual" ]] || die "Checksum mismatch for $tgz"
  ok "Checksum matches"
  tar -xzf "$tgz" -C "$PROJECT_ROOT/tools"
  rm -f "$tgz"
  ok "Spark installed at $SPARK_HOME"
fi

scala_jar="$(ls "$SPARK_HOME"/jars/scala-library-*.jar)"
[[ "$scala_jar" == *"$SPARK_SCALA_VERSION"* ]] \
  || die "Spark ships $(basename "$scala_jar") but conf/pipeline.env says $SPARK_SCALA_VERSION"
ok "Spark $(spark-submit --version 2>&1 | grep -m1 -o 'version [0-9.]*') / Scala $SPARK_SCALA_VERSION"
