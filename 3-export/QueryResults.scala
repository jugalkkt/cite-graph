package edu.cse412.citation

import edu.cse412.citation.Common._

/**
 * Ad-hoc SQL over the pipeline's HDFS outputs, for exploring results and answering
 * questions live during the demo. Registered views:
 *   papers      - one row per paper with every graph metric (stage 2 output)
 *   communities - one row per label-propagation community (stage 2 output)
 *   citations   - the clean edge list (stage 1 output)
 */
object QueryResults {

  def main(args: Array[String]): Unit = {
    val opts  = parseArgs(args)
    val spark = session("CitationQuery")
    spark.read.parquet(s"${opts.req("graph")}/paper_metrics").createOrReplaceTempView("papers")
    spark.read.parquet(s"${opts.req("graph")}/communities").createOrReplaceTempView("communities")
    spark.read.parquet(s"${opts.req("clean")}/edges").createOrReplaceTempView("citations")
    spark.sql(opts.req("sql")).show(opts.int("rows", 25), opts.int("truncate", 70))
    spark.stop()
  }
}
