package edu.cse412.citation

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/** Helpers shared by the ETL, graph and query entry points. */
object Common {
  val logger: Logger = LogManager.getLogger("edu.cse412.citation")

  /** Parses `--key value` pairs from the command line. */
  def parseArgs(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, s"expected --key value pairs, got: ${args.mkString(" ")}")
    args.grouped(2).map { case Array(k, v) =>
      require(k.startsWith("--"), s"bad option name: $k")
      k.drop(2) -> v
    }.toMap
  }

  implicit class Options(private val opts: Map[String, String]) extends AnyVal {
    def req(key: String): String = opts.getOrElse(key, sys.error(s"missing required option --$key"))
    def int(key: String, default: Int): Int = opts.get(key).map(_.toInt).getOrElse(default)
    def dbl(key: String, default: Double): Double = opts.get(key).map(_.toDouble).getOrElse(default)
  }

  def session(appName: String, conf: (String, String)*): SparkSession =
    conf.foldLeft(SparkSession.builder().appName(appName)) { case (b, (k, v)) => b.config(k, v) }.getOrCreate()

  /** Writes a small result table as a single CSV with a header, ready for `hdfs dfs -getmerge`. */
  def writeSummary(df: DataFrame, path: String): Unit =
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(path)

  /** Collects (metric, value) rows that are logged as they arrive and saved as one CSV per stage. */
  final class Metrics {
    private val rows = ArrayBuffer.empty[(String, String)]

    def add(name: String, value: Any): Unit = {
      rows += name -> value.toString
      logger.info(f"$name%-45s $value")
    }

    /** Runs `body` and records its wall-clock time; `body` must force evaluation (e.g. count). */
    def timed[T](name: String)(body: => T): T = {
      logger.info(s"-- running $name")
      val start  = System.nanoTime()
      val result = body
      add(s"seconds.$name", f"${(System.nanoTime() - start) / 1e9}%.1f")
      result
    }

    def toDF(spark: SparkSession): DataFrame = spark.createDataFrame(rows.toSeq).toDF("metric", "value")
  }
}
