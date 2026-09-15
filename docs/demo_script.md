# Demo Script (≤ 10 minutes)

A walkthrough that shows the pipeline really running end to end. Each segment lists who speaks, what is
on screen, and the exact commands to run. Rehearse once: the full run takes about 6 minutes once the
data is downloaded, so start stage 2 while explaining stage 1's output.

**Before recording:** HDFS running (`jps` shows NameNode and DataNode), dataset downloaded, jar built.
Wipe previous outputs so the run is visibly fresh:

```bash
hdfs dfs -rm -r -f -skipTrash /user/$USER/citation/clean /user/$USER/citation/graph
rm -rf results/tables results/charts
```

Close memory-heavy applications first: Spark refuses to start with less than 3.5 GB of RAM free.

---

## 0:00 – 1:00 · Problem and architecture (_Member 1_)

- Show `docs/architecture.png`.
- **Say:** "Citation counts treat every citation as equal. We use the structure of the network to rank
  influence, find research communities, and measure how far ideas spread, on 169K arXiv papers
  and 1.17M citations. The pipeline uses three tools: HDFS for storage, Spark for ETL, and GraphX
  for graph algorithms. Every hand-off goes through HDFS."

## 1:00 – 2:30 · Stage 0: data in HDFS (_Member 1_)

```bash
./scripts/02_start_hadoop.sh
./scripts/03_ingest_hdfs.sh            # shows the six files, sizes and fsck HEALTHY
hdfs dfs -cat /user/$USER/citation/raw/nodes/node_year.csv | head -3
hdfs dfs -cat /user/$USER/citation/raw/edges/edge.csv | head -3
```

- **Point out:** the year file has no id column (line number = paper id); the edge file doesn't say which
  side cites which; the titles came as a tar inside a `.gz`. Files are stored decompressed so they are
  splittable across Spark tasks.

## 2:30 – 4:00 · Stage 1: Spark ETL (_Member 2_)

```bash
./scripts/05_run_etl.sh
```

- Walk through the logged quality report as it prints:
  - all 169,343 papers have a year, a category and a title after the joins;
  - the edge list had no malformed, self, duplicate or dangling rows (the checks ran and found nothing to drop);
  - **direction inferred from data:** 87% of edges go from a newer paper to an older one, so `src cites dst`;
  - 20,903 citations point to a *newer* paper and 8,444 pairs cite each other, both caused by arXiv
    revisions. They are kept but flagged.
- Show the Parquet output: `hdfs dfs -ls /user/$USER/citation/clean`.

## 4:00 – 6:30 · Stage 2: GraphX (_Member 3_)

```bash
./scripts/06_run_graph.sh     # ≈ 4.5 min; narrate each algorithm as its log line appears
```

Optional while it runs: open the Spark UI at <http://localhost:4040> → Jobs, to show the Pregel
supersteps executing.

- **PageRank:** influence propagated along citations, validated against citation counts (Spearman ρ, top-k overlap).
- **Rank support (`aggregateMessages`):** how much of each paper's rank comes from its single biggest citer.
- **Connected components:** the giant component covers almost the whole graph.
- **Strongly connected components:** citation cycles, which should not exist in a time-ordered graph and come from revisions.
  Mention the fix: the DataFrame trimming shrinks the graph to a 44K-vertex core before GraphX SCC runs.
- **Label propagation:** communities from citation links alone, scored against arXiv subject labels and a random baseline.
- **Triangle count:** clustering far above a random graph of the same density.
- **Shortest paths:** how many papers build on each top paper within k hops.

## 6:30 – 8:30 · Results (_Member 2 and Member 3_)

```bash
./scripts/07_export_results.sh
column -s, -t < results/tables/top_pagerank.csv | head -12
column -s, -t < results/tables/hidden_gems.csv | head -8
```

- Show `results/charts/pagerank_vs_citations.png`, `community_validation.png`, `category_flows.png` and
  `landmark_reach.png`, giving one sentence of insight each (see the report's Results section).
- Live query to show the results are queryable:

```bash
./scripts/query.sh "SELECT title, year, in_degree, pagerank_rank FROM papers
                    WHERE category='cs.CL' ORDER BY pagerank_rank LIMIT 5"
```

## 8:30 – 10:00 · Honest scoping and wrap-up (_Member 1_)

- **Scale:** 1.17M edges fit on one laptop, but the same jobs run unchanged on a YARN cluster
  (`SPARK_MASTER=yarn`) and HDFS layout. ogbn-papers100M (1.6B edges) would need that.
- **Limits:** LPA is non-deterministic in tie-breaking; PageRank favours older papers (shown in the by-year chart);
  citations only count within the dataset.
- **Engineering lessons:** the local-mode scheduler hang, the Pregel-lineage memory blow-up, GraphX SCC
  exhausting RAM (fixed with trimming + a cgroup cap), and `/tmp` being RAM-backed.
- **Analytical honesty:** PageRank's #1 paper owes its rank to one citer (dropout has a single in-dataset
  reference), so 34% of the top 1% rest on one citer. We measured this and filtered derived lists.
