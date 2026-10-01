package edu.cse412.citation

import edu.cse412.citation.Common._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Stage 1 - Spark ETL (HDFS raw text -> HDFS Parquet).
 *
 * The raw ogbn-arxiv release is split over five files with three different layouts:
 * edges as "a,b" lines, years and labels as one value per line where the line number is
 * the paper id, and two header-carrying mapping files. Titles come from a separate TSV keyed
 * by Microsoft Academic Graph (MAG) id. This stage joins them into one vertex table, cleans
 * the edge list, decides the citation direction from the data, and records every row it
 * drops in a data-quality report.
 *
 * ogbn-mag (DATASET=mag) is ingested into the same HDFS layout. Its labels are publication venues
 * given as MAG ids, and it has no titles file, so its papers are named "MAG paper <id>".
 */
object CitationETL {

  def main(args: Array[String]): Unit = {
    val opts  = parseArgs(args)
    val raw   = opts.req("raw")
    val out   = opts.req("out")
    val spark = session("CitationETL")
    import spark.implicits._
    val m = new Metrics

    // Spark 4 runs in ANSI mode, where a bad cast throws. try_cast turns malformed values into NULL
    // so we can count and drop them instead of failing the whole job.
    def tryLong(expression: String) = expr(s"try_cast(trim($expression) AS BIGINT)")
    def tryInt(expression: String)  = expr(s"try_cast(trim($expression) AS INT)")

    // ---------------------------------------------------------------- vertices
    // node_year.csv / node_label.csv have no id column: line i describes paper i.
    // zipWithIndex numbers lines in file order across all HDFS blocks.
    def positional(path: String, name: String): DataFrame =
      spark.sparkContext.textFile(path).zipWithIndex()
        .map { case (line, idx) => (idx, line) }
        .toDF("id", "value")
        .select($"id", tryInt("value").as(name))

    val years  = positional(s"$raw/nodes/node_year.csv", "year").cache()
    val labels = positional(s"$raw/nodes/node_label.csv", "label").cache()
    val (yearRows, labelRows) = (years.count(), labels.count())
    m.add("vertices.year_rows", yearRows)
    m.add("vertices.label_rows", labelRows)
    require(yearRows == labelRows, s"node_year has $yearRows lines but node_label has $labelRows")

    val mapping = spark.read.option("header", "true").csv(s"$raw/mapping/nodeidx2paperid.csv")
      .toDF("id_raw", "mag_raw")
      .select(tryLong("id_raw").as("id"), tryLong("mag_raw").as("mag_id"))

    // Label names: ogbn-arxiv "arxiv cs na" -> "cs.NA"; ogbn-mag numeric MAG venue id -> "venue:1070115".
    // The file is labelidx2arxivcategory.csv or labelidx2venue.csv; resolve the name here, because Spark
    // logs a FileNotFoundException stack trace when handed a wildcard path.
    val labelNamesFile = {
      val pattern = new org.apache.hadoop.fs.Path(s"$raw/mapping/labelidx2*.csv")
      val found   = pattern.getFileSystem(spark.sparkContext.hadoopConfiguration).globStatus(pattern)
      require(found != null && found.length == 1, s"expected exactly one file matching $pattern")
      found.head.getPath.toString
    }
    val categories = spark.read.option("header", "true").csv(labelNamesFile)
      .toDF("label_raw", "name_raw")
      .select(tryInt("label_raw").as("label"), lower(trim($"name_raw")).as("name"))
      .select($"label",
              upper(regexp_extract($"name", "^arxiv cs ([a-z]{2})$", 1)).as("code"),
              regexp_extract($"name", "^([0-9]+)$", 1).as("venue"))
      .select($"label", when($"code" =!= "", concat(lit("cs."), $"code"))
                         .when($"venue" =!= "", concat(lit("venue:"), $"venue")).as("category"))

    // titleabs.tsv: mag_id <TAB> title <TAB> abstract. Abstracts can contain tabs, so split at most twice.
    // Only ogbn-arxiv ships titles; without the file, papers are named by their MAG id below.
    val titlesPath = new org.apache.hadoop.fs.Path(s"$raw/titles/titleabs.tsv")
    val titles = if (!titlesPath.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(titlesPath)) {
      m.add("titles.raw_rows", 0)
      Seq.empty[(Long, String)].toDF("mag_id", "title")
    } else {
      val titleRows = spark.read.text(titlesPath.toString)
        .select(split($"value", "\t", 3).as("f"))
        .select(expr("try_cast(trim(try_element_at(f, 1)) AS BIGINT)").as("mag_id"),
                regexp_replace(trim(expr("try_element_at(f, 2)")), "\\s+", " ").as("title"))
        .cache()
      m.add("titles.raw_rows", titleRows.count())
      titleRows
        .where($"mag_id".isNotNull && $"title".isNotNull && length($"title") > 0)
        .groupBy("mag_id").agg(first("title").as("title"))
    }

    val joined = years.join(labels, "id")
      .join(mapping, Seq("id"), "left")
      .join(categories, Seq("label"), "left")
      .join(titles, Seq("mag_id"), "left")
      .cache()

    val currentYear = java.time.Year.now.getValue
    val validVertex = $"year".between(1900, currentYear) && $"category".isNotNull && $"mag_id".isNotNull
    val valid = joined.where(validVertex)
    val vertices = valid
      .select($"id", $"mag_id", $"year", $"label", $"category",
              coalesce($"title", concat(lit("MAG paper "), $"mag_id")).as("title"))
      .cache()
    val vertexCount = vertices.count()
    m.add("vertices.dropped_invalid", joined.count() - vertexCount)
    m.add("vertices.clean", vertexCount)
    m.add("vertices.missing_title", valid.where($"title".isNull).count())
    m.add("vertices.categories", vertices.select("category").distinct().count())
    val yearRange = vertices.agg(min("year"), max("year")).first()
    m.add("vertices.year_range", s"${yearRange.getInt(0)}-${yearRange.getInt(1)}")

    // ------------------------------------------------------------------- edges
    val edgeLines = spark.read.text(s"$raw/edges/edge.csv")
    val parsed = edgeLines
      .select(split(trim($"value"), ",").as("f"))
      .select(expr("try_cast(trim(try_element_at(f, 1)) AS BIGINT)").as("src"),
              expr("try_cast(trim(try_element_at(f, 2)) AS BIGINT)").as("dst"),
              size($"f").as("fields"))
      .cache()
    val rawEdges = parsed.count()
    m.add("edges.1_raw_lines", rawEdges)

    val wellFormed = parsed.where($"src".isNotNull && $"dst".isNotNull && $"fields" === 2).select("src", "dst")
    val wellFormedCount = wellFormed.count()
    m.add("edges.2_dropped_malformed", rawEdges - wellFormedCount)

    val noSelf = wellFormed.where($"src" =!= $"dst")
    val noSelfCount = noSelf.count()
    m.add("edges.3_dropped_self_citation", wellFormedCount - noSelfCount)

    val distinctEdges = noSelf.distinct().cache()
    val distinctCount = distinctEdges.count()
    m.add("edges.4_dropped_duplicate", noSelfCount - distinctCount)

    // Both endpoints must be clean vertices; anything else is a dangling reference.
    val ids = vertices.select("id")
    val resolved = distinctEdges
      .join(ids.withColumnRenamed("id", "src"), Seq("src"), "left_semi")
      .join(ids.withColumnRenamed("id", "dst"), Seq("dst"), "left_semi")
    val withYears = resolved
      .join(vertices.select($"id".as("src"), $"year".as("src_year")), "src")
      .join(vertices.select($"id".as("dst"), $"year".as("dst_year")), "dst")
      .cache()
    val resolvedCount = withYears.count()
    m.add("edges.5_dropped_dangling", distinctCount - resolvedCount)

    // OGB only says an edge means "one paper cites another". A citing paper is almost always the
    // newer one, so the dominant year order tells us which column is the citing side.
    val forward  = withYears.where($"src_year" > $"dst_year").count()
    val backward = withYears.where($"src_year" < $"dst_year").count()
    m.add("direction.src_newer_than_dst", forward)
    m.add("direction.src_older_than_dst", backward)
    m.add("direction.same_year", resolvedCount - forward - backward)
    val flip = backward > forward
    m.add("direction.decision", if (flip) "flipped: dst cites src" else "kept: src cites dst")
    val oriented =
      if (flip) withYears.select($"dst".as("src"), $"src".as("dst"), $"dst_year".as("src_year"), $"src_year".as("dst_year"))
      else withYears

    // Citing a *newer* paper happens on arXiv when a paper is revised after publication.
    // These are real citations, so we keep them but flag them for the analysis stage.
    val edges = oriented
      .withColumn("cites_newer_paper", $"src_year" < $"dst_year")
      .select("src", "dst", "src_year", "dst_year", "cites_newer_paper")
      .cache()
    m.add("edges.6_clean", edges.count())
    m.add("edges.cites_newer_paper", edges.where($"cites_newer_paper").count())
    val reciprocal = edges.as("a")
      .join(edges.as("b"), $"a.src" === $"b.dst" && $"a.dst" === $"b.src")
      .count() / 2
    m.add("edges.reciprocal_pairs", reciprocal)

    // ------------------------------------------------------------------ output
    vertices.write.mode(SaveMode.Overwrite).parquet(s"$out/vertices")
    edges.write.mode(SaveMode.Overwrite).parquet(s"$out/edges")
    writeSummary(m.toDF(spark), s"$out/quality_report")
    logger.info(s"Clean graph written to $out ($vertexCount papers, ${edges.count()} citations)")
    spark.stop()
  }
}
