package edu.cse412.citation

import edu.cse412.citation.Common._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.{LabelPropagation, ShortestPaths}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * Stage 2 - GraphX analytics (HDFS Parquet -> GraphX -> HDFS Parquet/CSV).
 *
 * Builds a property graph where each vertex is a paper and each edge means "src cites dst",
 * then runs Pregel-style algorithms that tabular tools cannot express in a single pass:
 * PageRank, weakly/strongly connected components, label propagation, triangle counting and
 * multi-source shortest paths. Every algorithm's output is validated against something
 * independent (raw citation counts, the papers' publication venues, a random baseline).
 */
object CitationGraphAnalytics {

  /** Vertex property carried through the GraphX algorithms. */
  final case class Paper(year: Int, label: Int)

  def main(args: Array[String]): Unit = {
    val opts       = parseArgs(args)
    val clean      = opts.req("clean")
    val out        = opts.req("out")
    val prTol      = opts.dbl("pagerank-tol", 1e-4)
    val prReset    = opts.dbl("pagerank-reset", 0.15)
    val lpaSteps   = opts.int("lpa-steps", 10)
    val sccIters   = opts.int("scc-iters", 10)
    val nLandmarks = opts.int("landmarks", 5)

    // Iterative Pregel algorithms grow the RDD lineage by a few stages per superstep; without
    // periodic checkpoints the driver's DAG bookkeeping fills the heap. Checkpoint to HDFS every N steps.
    implicit val spark: SparkSession = session("CitationGraphAnalytics",
      "spark.graphx.pregel.checkpointInterval" -> opts.getOrElse("checkpoint-interval", "5"))
    import spark.implicits._
    spark.sparkContext.setCheckpointDir(s"$out/_checkpoints")
    val m = new Metrics
    val summary = (name: String, df: DataFrame) => writeSummary(df, s"$out/summary/$name")

    val vertices = spark.read.parquet(s"$clean/vertices").cache()
    val edges    = spark.read.parquet(s"$clean/edges").cache()

    // ------------------------------------------------------- 1. property graph
    val graph: Graph[Paper, Int] = m.timed("build_graph") {
      val vRdd: RDD[(VertexId, Paper)] = vertices.select("id", "year", "label").as[(Long, Int, Int)].rdd
        .map { case (id, year, label) => (id, Paper(year, label)) }
      val eRdd: RDD[Edge[Int]] = edges.select("src", "dst").as[(Long, Long)].rdd
        .map { case (src, dst) => Edge(src, dst, 1) }
      val g = Graph(vRdd, eRdd, Paper(-1, -1), StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)
        .partitionBy(PartitionStrategy.EdgePartition2D)
        .cache()
      g.edges.count()
      g
    }
    val n = graph.numVertices
    val e = graph.numEdges
    m.add("graph.vertices", n)
    m.add("graph.edges", e)
    m.add("graph.density", f"${e.toDouble / (n.toDouble * (n - 1))}%.3e")

    // ------------------------------------------------------------- 2. degrees
    val degrees = m.timed("degrees") {
      val d = vertices.select("id")
        .join(graph.inDegrees.toDF("id", "in_degree"), Seq("id"), "left")
        .join(graph.outDegrees.toDF("id", "out_degree"), Seq("id"), "left")
        .na.fill(0L, Seq("in_degree", "out_degree"))
        .cache()
      d.count()
      d
    }
    val degreeStats = degrees.agg(max("in_degree"), avg("in_degree"), max("out_degree"),
      avg(($"in_degree" === 0).cast("double")), avg(($"out_degree" === 0).cast("double"))).first()
    m.add("degree.max_in", degreeStats.get(0))
    m.add("degree.mean_in", f"${degreeStats.getDouble(1)}%.2f")
    m.add("degree.max_out", degreeStats.get(2))
    m.add("degree.share_never_cited", f"${degreeStats.getDouble(3)}%.4f")
    m.add("degree.share_no_references", f"${degreeStats.getDouble(4)}%.4f")
    summary("in_degree_distribution", degrees.groupBy("in_degree").agg(count(lit(1)).as("papers")).orderBy("in_degree"))
    summary("out_degree_distribution", degrees.groupBy("out_degree").agg(count(lit(1)).as("papers")).orderBy("out_degree"))

    // ------------------------------------------------------------ 3. PageRank
    // Rank flows along "src cites dst", so it accumulates at papers cited by influential papers.
    val pagerank = m.timed("pagerank") {
      val raw   = graph.pageRank(prTol, prReset).vertices.toDF("id", "pr_raw")
      val total = raw.agg(sum("pr_raw")).first().getDouble(0)
      val pr    = raw.select($"id", ($"pr_raw" / total).as("pagerank")).cache()
      pr.count()
      pr
    }

    // Validation: how much does PageRank agree with plain citation counts?
    val ranked = degrees.join(pagerank, "id")
      .join(averageRank(degrees, "in_degree", "citation_rank"), "id")
      .join(averageRank(pagerank, "pagerank", "pagerank_rank"), "id")
      .cache()
    m.add("pagerank.spearman_vs_citations", f"${ranked.stat.corr("citation_rank", "pagerank_rank")}%.4f")
    degrees.unpersist()
    pagerank.unpersist()
    for (k <- Seq(100, 1000)) {
      val overlap = ranked.where($"pagerank_rank" <= k && $"citation_rank" <= k).count()
      m.add(s"pagerank.top${k}_overlap_with_top${k}_cited", overlap)
    }

    // Where does each paper's PageRank come from? A citing paper u passes PR(u) / outdeg(u) to every
    // paper it cites. aggregateMessages sums those contributions per paper and keeps the largest one,
    // giving the share supplied by the single biggest citer. The dataset only keeps references to papers
    // inside it, so many papers have one or two references left; a highly ranked paper with a single reference
    // hands *all* its rank to it. A share near 1 flags rank that rests on one citing paper.
    val support = m.timed("rank_support") {
      val prOut = graph
        .outerJoinVertices(ranked.select("id", "pagerank").as[(Long, Double)].rdd) { (_, _, pr) => pr.getOrElse(0.0) }
        .outerJoinVertices(graph.outDegrees) { (_, pr, deg) => (pr, deg.getOrElse(0)) }
      val s = prOut.aggregateMessages[(Double, Double)](
          ctx => { val c = ctx.srcAttr._1 / ctx.srcAttr._2; ctx.sendToDst((c, c)) },
          (a, b) => (a._1 + b._1, math.max(a._2, b._2)))
        .map { case (id, (total, top)) => (id, top / total) }
        .toDF("id", "top_citer_share")
        .cache()
      s.count()
      s
    }
    // "Well supported" influence: cited at least 10 times and no single citer supplies most of the rank.
    val supported = ranked.join(support, Seq("id"), "left")
      .withColumn("well_supported", coalesce($"in_degree" >= 10 && $"top_citer_share" <= 0.5, lit(false)))
      .cache()
    val top1 = supported.where($"pagerank_rank" <= n / 100).cache()
    m.add("pagerank.top1pct_papers", top1.count())
    m.add("pagerank.top1pct_with_le5_citations", top1.where($"in_degree" <= 5).count())
    m.add("pagerank.top1pct_single_citer_majority", top1.where($"top_citer_share" > 0.5).count())
    m.add("pagerank.top1pct_well_supported", top1.where($"well_supported").count())

    // ------------------------------------------- 4. weakly connected components
    val wcc = m.timed("connected_components") {
      val c = graph.connectedComponents().vertices.toDF("id", "wcc").cache()
      c.count()
      c
    }
    val wccSizes = wcc.groupBy("wcc").agg(count(lit(1)).as("size")).cache()
    val largestWcc = wccSizes.agg(max("size")).first().getLong(0)
    m.add("wcc.components", wccSizes.count())
    m.add("wcc.largest_size", largestWcc)
    m.add("wcc.largest_share", f"${largestWcc.toDouble / n}%.4f")
    summary("wcc_size_distribution",
      wccSizes.groupBy("size").agg(count(lit(1)).as("components")).orderBy(desc("size")))

    // ----------------------------------------- 5. strongly connected components
    // A citation graph should be acyclic (you can only cite the past). Any SCC with more than
    // one paper is a citation cycle, which comes from papers revised after publication or mis-dated.
    //
    // GraphX's built-in SCC repeatedly trims vertices that have no in- or out-edge, caching a new copy
    // of the graph every round. A near-acyclic graph needs many rounds, and running it on the full graph
    // exhausted the laptop's RAM. We do that trimming first with DataFrame joins checkpointed to HDFS
    // (cyclicCore), then run GraphX SCC only on the small core that can contain cycles. Same result.
    val scc = if (sccIters <= 0) vertices.select($"id", $"id".as("scc")) else m.timed("strongly_connected_components") {
      val core = cyclicCore(edges.select("src", "dst"), m)
      val coreGraph = Graph.fromEdgeTuples(core.as[(Long, Long)].rdd, defaultValue = 0,
        edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val coreScc = coreGraph.stronglyConnectedComponents(sccIters).vertices.toDF("id", "scc")
      val c = vertices.select("id").join(coreScc, Seq("id"), "left")
        .select($"id", coalesce($"scc", $"id").as("scc"))
        .cache()
      c.count()
      coreGraph.unpersist()
      c
    }
    val sccSizes = scc.groupBy("scc").agg(count(lit(1)).as("size")).where($"size" > 1).cache()
    m.add("scc.cycles", sccSizes.count())
    m.add("scc.papers_in_cycles", sccSizes.agg(coalesce(sum("size"), lit(0L))).first().get(0))
    m.add("scc.largest_size", sccSizes.agg(coalesce(max("size"), lit(0L))).first().get(0))

    // --------------------------------------------- 6. label propagation (LPA)
    val communities = m.timed("label_propagation") {
      val c = LabelPropagation.run(graph, lpaSteps).vertices.toDF("id", "community").cache()
      c.count()
      c
    }
    val communitySizes = communities.groupBy("community").agg(count(lit(1)).as("size")).cache()
    m.add("lpa.communities", communitySizes.count())
    m.add("lpa.communities_size_ge_10", communitySizes.where($"size" >= 10).count())
    m.add("lpa.largest_size", communitySizes.agg(max("size")).first().get(0))
    m.add("lpa.share_in_communities_ge_100",
      f"${communitySizes.where($"size" >= 100).agg(coalesce(sum("size"), lit(0L))).first().getLong(0).toDouble / n}%.4f")

    // Validation: do the communities found from citations alone match the publication venues?
    val labelled = communities.join(vertices.select("id", "category"), "id").cache()
    val (nmi, ari, purity) = partitionAgreement(labelled, "community", "category")
    m.add("lpa.nmi_vs_category", f"$nmi%.4f")
    m.add("lpa.ari_vs_category", f"$ari%.4f")
    m.add("lpa.purity_vs_category", f"$purity%.4f")
    // Baseline: the same community sizes assigned at random. NMI and purity both grow with the number
    // of clusters, so this tells us how much of the score is structure and how much is chance.
    val (nmi0, ari0, purity0) = partitionAgreement(randomRelabel(labelled, "community", "category"), "community", "category")
    m.add("lpa.baseline_nmi", f"$nmi0%.4f")
    m.add("lpa.baseline_ari", f"$ari0%.4f")
    m.add("lpa.baseline_purity", f"$purity0%.4f")

    // ------------------------------------------------------ 7. triangle count
    val triangles = m.timed("triangle_count") {
      val t = graph.triangleCount().vertices.toDF("id", "triangles").cache()
      t.count()
      t
    }
    // Clustering is defined on the undirected graph: count each neighbour once.
    val undirected = edges.select(least($"src", $"dst").as("a"), greatest($"src", $"dst").as("b")).distinct().cache()
    val neighbours = undirected.select($"a".as("id")).union(undirected.select($"b".as("id")))
      .groupBy("id").agg(count(lit(1)).as("neighbors"))
    val clustering = vertices.select("id").join(triangles, "id").join(neighbours, Seq("id"), "left")
      .na.fill(0L, Seq("neighbors"))
      .withColumn("clustering", when($"neighbors" >= 2,
        $"triangles" * 2.0 / ($"neighbors".cast("double") * ($"neighbors" - 1))).otherwise(0.0))
      .cache()
    val triStats = clustering.agg(sum("triangles"), avg("clustering"),
      sum($"neighbors".cast("double") * ($"neighbors" - 1) / 2)).first()
    val totalTriangles = triStats.getLong(0) / 3
    val undirectedEdges = undirected.count()
    m.add("triangles.total", totalTriangles)
    m.add("triangles.avg_clustering", f"${triStats.getDouble(1)}%.4f")
    m.add("triangles.transitivity", f"${3.0 * totalTriangles / triStats.getDouble(2)}%.4f")
    // For a random graph with the same density, expected clustering equals the edge probability.
    m.add("triangles.random_graph_clustering", f"${2.0 * undirectedEdges / (n.toDouble * (n - 1))}%.2e")

    // ---------------------------------- 8. knowledge reach of the top papers
    // ShortestPaths sends distances from dst back to src, so hops(p, L) is the length of the
    // shortest citation chain from paper p down to landmark L: generation 1 cites L directly,
    // generation 2 cites a paper that cites L, and so on.
    val landmarks = supported.where($"well_supported").orderBy($"pagerank_rank").limit(nLandmarks)
      .select("id").as[Long].collect().toSeq
    val reach = m.timed("shortest_paths") {
      val r = ShortestPaths.run(graph, landmarks).vertices
        .flatMap { case (_, dist) => dist.iterator.collect { case (landmark, hops) if hops > 0 => (landmark, hops) } }
        .toDF("landmark", "hops")
        .groupBy("landmark", "hops").agg(count(lit(1)).as("papers"))
        .cache()
      r.count()
      r
    }

    // --------------------------------------------- 9. citation flow by field
    // Uses the GraphX triplet view: each edge with both endpoints' properties attached.
    val flows = m.timed("category_flows") {
      val f = graph.triplets
        .map(t => ((t.srcAttr.label, t.dstAttr.label), 1L))
        .reduceByKey(_ + _)
        .map { case ((src, dst), c) => (src, dst, c) }
        .toDF("src_label", "dst_label", "citations")
        .cache()
      f.count()
      f
    }
    val withinField = flows.where($"src_label" === $"dst_label").agg(sum("citations")).first().getLong(0)
    m.add("fields.within_field_citation_share", f"${withinField.toDouble / e}%.4f")

    // ===================================================== assemble outputs
    val papers = vertices
      .join(supported, "id")
      .join(wcc, "id")
      .join(scc, "id")
      .join(communities, "id")
      .join(clustering.select("id", "triangles", "neighbors", "clustering"), "id")
      .withColumn("pagerank_x_avg", $"pagerank" * n)
      .cache()
    papers.write.mode(SaveMode.Overwrite).parquet(s"$out/paper_metrics")
    val paperCols = Seq("pagerank_rank", "title", "year", "category", "in_degree", "citation_rank", "pagerank_x_avg",
      "top_citer_share", "well_supported")
      .map(col)

    summary("top_pagerank", papers.orderBy($"pagerank_rank").limit(25).select(paperCols: _*))
    summary("top_cited", papers.orderBy($"citation_rank", $"pagerank_rank").limit(25).select(paperCols: _*))

    // Hidden gems: in the top 1% by PageRank and well supported, but ranked much lower by raw citation count.
    summary("hidden_gems", papers.where($"pagerank_rank" <= n / 100 && $"well_supported")
      .withColumn("rank_lift", $"citation_rank" / $"pagerank_rank")
      .orderBy(desc("rank_lift")).limit(20)
      .select((paperCols :+ $"rank_lift"): _*))

    // Rising stars: most influential papers from the last two years of the dataset.
    val maxYear = vertices.agg(max("year")).first().getInt(0)
    summary("rising_stars", papers.where($"year" >= maxYear - 1)
      .orderBy($"pagerank_rank").limit(20).select(paperCols: _*))

    val topShare = papers.select(($"pagerank_rank" <= n / 100).cast("double").as("top1"), $"year")
    summary("by_year", papers.groupBy("year").agg(
        count(lit(1)).as("papers"),
        round(avg("in_degree"), 3).as("mean_in_degree"),
        round(avg("pagerank_x_avg"), 4).as("mean_pagerank_x_avg"))
      .join(topShare.groupBy("year").agg(round(avg("top1"), 5).as("share_in_top1pct_pagerank")), "year")
      .orderBy("year"))

    // 2-D histogram (log10 bins) of citation count vs PageRank over every paper, for the agreement chart.
    summary("pagerank_vs_citations", papers
      .select((floor(log10($"in_degree" + 1) * 10) / 10).as("log10_citations_plus1"),
              (floor(log10($"pagerank_x_avg") * 10) / 10).as("log10_pagerank_x_avg"))
      .groupBy("log10_citations_plus1", "log10_pagerank_x_avg").agg(count(lit(1)).as("papers")))

    val catNames = vertices.select("label", "category").distinct().cache()
    val namedFlows = flows
      .join(catNames.toDF("src_label", "citing_category"), "src_label")
      .join(catNames.toDF("dst_label", "cited_category"), "dst_label")
      .select("citing_category", "cited_category", "citations")
      .cache()
    summary("category_flows", namedFlows.orderBy(desc("citations")))

    val outgoing = namedFlows.groupBy("citing_category").agg(
      sum("citations").as("citations_made"),
      sum(when($"citing_category" === $"cited_category", $"citations").otherwise(0L)).as("within"))
    val topPaperPerCategory = papers
      .withColumn("r", row_number().over(Window.partitionBy("category").orderBy($"well_supported".desc, $"pagerank_rank")))
      .where($"r" === 1).select($"category", $"title".as("most_influential_paper"))
    summary("categories", papers.groupBy("category").agg(
        count(lit(1)).as("papers"),
        round(avg("in_degree"), 3).as("mean_in_degree"),
        round(avg("pagerank_x_avg"), 4).as("mean_pagerank_x_avg"))
      .join(outgoing.withColumnRenamed("citing_category", "category"), Seq("category"), "left")
      .withColumn("within_field_share", round($"within" / $"citations_made", 4))
      .drop("within")
      .join(topPaperPerCategory, Seq("category"), "left")
      .orderBy(desc("papers")))

    // Largest LPA communities: what field dominates each, and how cleanly.
    val byCommunityCategory = labelled.groupBy("community", "category").agg(count(lit(1)).as("c"))
    val dominant = byCommunityCategory
      .withColumn("r", row_number().over(Window.partitionBy("community").orderBy(desc("c"), $"category")))
      .where($"r" === 1).select($"community", $"category".as("dominant_category"), $"c".as("dominant_count"))
    val communityTop = papers
      .withColumn("r", row_number().over(Window.partitionBy("community").orderBy($"well_supported".desc, $"pagerank_rank")))
      .where($"r" === 1).select($"community", $"title".as("most_influential_paper"))
    val communityTable = communitySizes
      .join(dominant, "community")
      .join(papers.groupBy("community").agg(
        min("year").as("first_year"), expr("percentile_approx(year, 0.5)").as("median_year")), "community")
      .join(communityTop, "community")
      .withColumn("dominant_share", round($"dominant_count" / $"size", 4))
      .drop("dominant_count")
      .cache()
    communityTable.write.mode(SaveMode.Overwrite).parquet(s"$out/communities")
    summary("top_communities", communityTable.orderBy(desc("size")).limit(25))
    summary("community_size_distribution",
      communitySizes.groupBy("size").agg(count(lit(1)).as("communities")).orderBy("size"))

    summary("largest_cycles", sccSizes.orderBy(desc("size")).limit(10)
      .join(papers.select("scc", "title", "year", "category"), "scc")
      .groupBy("scc", "size").agg(
        concat_ws(" | ", slice(sort_array(collect_list(concat($"year".cast("string"), lit(" "), $"title"))), 1, 4))
          .as("example_papers"))
      .orderBy(desc("size")))

    summary("landmark_reach", reach
      .join(papers.select($"id".as("landmark"), $"title", $"year", $"pagerank_rank"), "landmark")
      .withColumn("cumulative_papers", sum("papers").over(
        Window.partitionBy("landmark").orderBy("hops").rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .orderBy($"pagerank_rank", $"hops"))

    summary("graph_stats", m.toDF(spark))
    logger.info(s"GraphX results written to $out")
    spark.stop()
  }

  /**
   * Repeatedly removes vertices that have no incoming or no outgoing edge among the remaining edges.
   * Such a vertex cannot lie on a directed cycle, so what survives contains every cycle (and every
   * strongly connected component larger than one vertex). Each round is checkpointed to HDFS, which
   * cuts the lineage and keeps the shrinking edge list on disk instead of in memory.
   */
  def cyclicCore(edges: DataFrame, m: Metrics)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    var core     = edges.checkpoint()
    var size     = core.count()
    var previous = -1L
    var rounds   = 0
    while (size != previous) {
      previous = size
      val alive = core.select($"src".as("id")).distinct()
        .join(core.select($"dst".as("id")).distinct(), "id")
      core = core
        .join(alive.withColumnRenamed("id", "src"), Seq("src"), "left_semi")
        .join(alive.withColumnRenamed("id", "dst"), Seq("dst"), "left_semi")
        .checkpoint()
      size = core.count()
      rounds += 1
    }
    m.add("scc.trim_rounds", rounds)
    m.add("scc.core_edges", size)
    m.add("scc.core_vertices", core.select($"src".as("id")).union(core.select($"dst".as("id"))).distinct().count())
    core
  }

  /**
   * 1-based rank by descending value where tied values share their mean rank (Spearman ties).
   * Uses a distributed sort plus zipWithIndex instead of a global window, which would pull
   * every row into one partition.
   */
  def averageRank(df: DataFrame, valueCol: String, rankCol: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val positioned = df.select($"id", col(valueCol).cast("double").as("v")).as[(Long, Double)].rdd
      .sortBy(-_._2)
      .zipWithIndex()
      .map { case ((id, v), pos) => (id, v, pos + 1) }
      .toDF("id", "v", "pos")
    val tieRanks = positioned.groupBy("v").agg(avg("pos").as(rankCol))
    positioned.join(tieRanks, "v").select("id", rankCol)
  }

  /** Agreement between a predicted partition and a ground-truth one: (NMI, ARI, purity). */
  def partitionAgreement(df: DataFrame, predicted: String, truth: String): (Double, Double, Double) = {
    val cells = df.groupBy(predicted, truth).agg(count(lit(1)).cast("double").as("nij")).cache()
    val rowsA = cells.groupBy(predicted).agg(sum("nij").as("ai"))
    val rowsB = cells.groupBy(truth).agg(sum("nij").as("bj"))
    val total = cells.agg(sum("nij")).first().getDouble(0)
    def scalar(frame: DataFrame, c: Column): Double = frame.agg(c).first().getDouble(0)
    def entropy(frame: DataFrame, c: String): Double = scalar(frame, sum(-(col(c) / total) * log(col(c) / total)))
    def pairs(c: Column): Column = c * (c - 1) / 2

    // Normalised mutual information (arithmetic mean normalisation, as in scikit-learn).
    val mutualInfo = scalar(cells.join(rowsA, predicted).join(rowsB, truth),
      sum((col("nij") / total) * log(col("nij") * total / (col("ai") * col("bj")))))
    val hA  = entropy(rowsA, "ai")
    val hB  = entropy(rowsB, "bj")
    val nmi = if (hA + hB == 0) 1.0 else 2 * mutualInfo / (hA + hB)

    // Adjusted Rand index.
    val sumCells = scalar(cells, sum(pairs(col("nij"))))
    val sumA     = scalar(rowsA, sum(pairs(col("ai"))))
    val sumB     = scalar(rowsB, sum(pairs(col("bj"))))
    val expected = sumA * sumB / (total * (total - 1) / 2)
    val ari      = (sumCells - expected) / (0.5 * (sumA + sumB) - expected)

    val purity = scalar(cells.groupBy(predicted).agg(max("nij").as("best")), sum("best")) / total
    cells.unpersist()
    (nmi, ari, purity)
  }

  /**
   * Shuffles the `predicted` labels across rows while keeping their size distribution:
   * the chance-level baseline for partitionAgreement. Deterministic (ordered by a hash of id).
   */
  def randomRelabel(df: DataFrame, predicted: String, truth: String)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val shuffled = df.select(xxhash64($"id").as("k"), col(predicted)).as[(Long, Long)].rdd
      .sortByKey().values.zipWithIndex().map(_.swap)
    val truthInOrder = df.select($"id", col(truth)).as[(Long, String)].rdd
      .sortByKey().values.zipWithIndex().map(_.swap)
    shuffled.join(truthInOrder).values.toDF(predicted, truth)
  }
}
