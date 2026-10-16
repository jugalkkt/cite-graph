# Academic Citation Network Analyzer

**CSE412 Big Data & Large-Scale Computing: Applied Project (Track C: Graph Analytics)**

An end-to-end big-data pipeline that loads the citation network of **ogbn-mag** (736,389 papers from all
sciences and 5,416,271 citations) into **HDFS**. **Apache Spark** cleans and joins it, and **Spark GraphX**
runs graph algorithms to find which papers are most influential, whether citation communities match
publication venues, and how far ideas spread through the citation graph.

## Key results

| Question | Finding |
|---|---|
| Does PageRank agree with citation counts? | Overall yes (Spearman ρ = 0.84), but only 22 of the top 100 overlap (295 of the top 1,000) |
| Can PageRank be trusted? | 29% of the top-1% PageRank papers get most of their rank from a single citing paper (e.g. #6 has 177 citations, 88% of its rank from one citer). Measured with GraphX `aggregateMessages`; hidden gems and landmarks use only well-supported papers |
| Do citation communities match venues? | Label propagation vs the 349 publication venues: NMI 0.42 (random baseline 0.17), ARI 0.04 (0.00), purity 0.36 (0.10). Above chance, but venues mix topics, so agreement is partial |
| How inward-looking are venues? | 30.4% of citations stay within the citing paper's venue; among venues with 5,000+ papers this ranges from 59% to 12% |
| Is the graph acyclic, as time would suggest? | No: 5,770 citation cycles covering 18.2% of papers, one of them 120,104 papers large |
| How far do top papers reach? | The most influential paper is reached by 11.2% of all papers within 3 citation hops, 35.4% within 10; each landmark is eventually reached by about 43% |

Results are in [results/](results/) (`tables/` and `charts/`).

## Tools and versions

| Tool | Version | Role in the pipeline |
|---|---|---|
| **HDFS** (Apache Hadoop) | 3.3.6 | Distributed storage for every stage: raw text in, Parquet between stages, results out |
| **Apache Spark** (core + SQL) | 4.0.4 (Scala 2.13.16) | Stage 1 ETL: parse, join, validate and orient the raw data; write Parquet |
| **Spark GraphX** | 4.0.4 | Stage 2: PageRank, connected components, SCC, label propagation, triangle count, shortest paths |
| Java (OpenJDK) | 21 | Runtime for Hadoop and Spark |
| scala-cli | 1.17.1 | Builds the Spark application jar |
| Python 3 + matplotlib | 3.13 / 3.10 | Draws charts from the exported summary tables (no analysis) |

Data moves between the tools only through HDFS:
`HDFS /raw → Spark ETL → HDFS /clean (Parquet) → GraphX → HDFS /graph (Parquet + CSV) → results`.

## Dataset

[ogbn-mag](https://ogb.stanford.edu/docs/nodeprop/#ogbn-mag) from the Open Graph Benchmark, a subset of the
Microsoft Academic Graph (MAG). It is a heterogeneous graph (papers, authors, institutions, fields of study);
this project uses its **paper** vertices and **paper-cites-paper** edges. Each paper has a publication year
(2010–2019) and one of 349 publication venues. The release has no titles or venue names, so papers appear as
`MAG paper <id>` and venues as `venue:<MAG venue id>`. The ids can be looked up in
[OpenAlex](https://openalex.org), e.g. `https://api.openalex.org/works/mag:<id>`.

| Raw file (inside `mag.zip`) | Rows | Layout quirk the ETL handles |
|---|---|---|
| `raw/relations/paper___cites___paper/edge.csv.gz` | 5,416,271 | `a,b` pairs; the ETL checks the citation direction against publication years |
| `raw/node-feat/paper/node_year.csv.gz`, `raw/node-label/paper/node-label.csv.gz` | 736,389 each | No id column: line *i* belongs to paper *i* |
| `mapping/paper_entidx2name.csv.gz` | 736,389 | Header row; maps graph id → MAG paper id |
| `mapping/labelidx2venuename.csv.gz` | 349 | Header row; maps label index → MAG venue id |

The zip is 412 MB, most of it a 332 MB paper-feature file that this pipeline does not use and does not unpack.
In HDFS the five raw files take 89 MB.

## Project structure

Each pipeline stage has its own numbered folder holding both its run script and its code, so you can read
the project in the same order the data flows through it.

```
run_pipeline.sh                  runs every step below, in order
conf/
  pipeline.env                   all settings: paths, HDFS layout, memory, algorithm parameters
  log4j2.properties              keeps Spark's log output short
  local.env                      optional per-machine overrides (not in git)
common/                          shared setup, used by every stage
  common.sh                      shell helpers: loads the settings, starts Spark with a memory cap
  Common.scala                   Scala helpers: argument parsing, metrics, writing CSV summaries
  project.scala                  Scala build settings (Spark 4.0.4, Scala 2.13)
  setup_tools.sh                 checks Java/Hadoop/scala-cli, downloads Spark into ./tools
  start_hadoop.sh                starts HDFS
  build.sh                       compiles all Scala code into target/citation-analyzer.jar
0-ingest/                        stage 0: get the data into HDFS
  download_data.sh               downloads ogbn-mag into ./data/raw
  ingest_hdfs.sh                 decompresses the raw files and streams them into HDFS /raw
1-etl/                           stage 1: Spark cleans the data          (HDFS /raw → /clean)
  run_etl.sh
  CitationETL.scala
2-graph/                         stage 2: GraphX graph algorithms        (HDFS /clean → /graph)
  run_graph.sh
  CitationGraphAnalytics.scala
3-export/                        stage 3: results out of HDFS            (HDFS /graph → results/)
  export_results.sh              copies the summary tables out and draws the charts
  make_charts.py                 the chart drawing (no analysis)
  query.sh                       ad-hoc Spark SQL over the results in HDFS
  QueryResults.scala             the Spark side of query.sh
results/                         output: tables/ (CSV) and charts/ (PNG)
```

Folders created when the pipeline runs (not in git): `tools/` (Spark), `data/` (downloads and Spark scratch
space) and `target/` (the compiled jar).

## Prerequisites

- Linux with **Java 17+** (Java 21 tested). If `java -version` shows Java 8, set
  `CITATION_JAVA_HOME=/path/to/jdk-21` first. The scripts pin Java 21 by default.
- **Hadoop 3.x** with a working single-node HDFS (`fs.defaultFS` set in `core-site.xml`). `HADOOP_HOME`
  defaults to `~/hadoop`.
- **scala-cli** ([install](https://scala-cli.virtuslab.org/install)).
- **Python 3** with **matplotlib** (charts only).
- About 2 GB of free disk space and **8 GB of free RAM**. Spark runs with a 6 GB heap inside a systemd
  cgroup capped at 8 GB (`MEMORY_CAP_MB`), so a runaway job can only kill itself, never other applications.
  Scripts refuse to start Spark when less RAM than that is available.

Spark itself does **not** need to be installed: `common/setup_tools.sh` downloads it into `./tools`.

## Running it

```bash
./run_pipeline.sh            # everything, end to end: 19 min 47 s measured with the data already downloaded
```

Measured on a 4-core laptop: download ≈ 3.5 min (first run only), ingest ≈ 25 s, Spark ETL ≈ 1 min,
GraphX ≈ 17.5 min (8.3 min of it strongly connected components), export ≈ 35 s.

Or one stage at a time (useful for the demo):

```bash
./common/setup_tools.sh        # Spark into ./tools
./0-ingest/download_data.sh    # dataset into ./data/raw
./common/start_hadoop.sh       # HDFS up
./0-ingest/ingest_hdfs.sh      # raw files → HDFS /user/$USER/citation-mag/raw
./common/build.sh              # Scala → target/citation-analyzer.jar
./1-etl/run_etl.sh             # Spark: /raw → /clean
./2-graph/run_graph.sh         # GraphX: /clean → /graph
./3-export/export_results.sh   # /graph summaries → results/
```

Each step is idempotent, so re-running skips work that is already done. Use `FORCE=1 ./0-ingest/ingest_hdfs.sh`
to overwrite the raw files in HDFS.

### Querying the results

```bash
./3-export/query.sh "SELECT title, year, in_degree, pagerank_rank FROM papers
                    WHERE category = 'venue:1980519' ORDER BY pagerank_rank LIMIT 10"
```

These views are available: `papers` (one row per paper with every metric), `communities` (one row per
label-propagation community) and `citations` (clean edges).

### Configuration

All knobs live in `conf/pipeline.env` and can be overridden from the environment, e.g.
`LPA_MAX_STEPS=20 ./2-graph/run_graph.sh`. Machine-specific values (e.g. `HADOOP_HOME`, `RESULTS_DIR`)
can go in an optional, git-ignored `conf/local.env`, which is loaded before `pipeline.env`.

| Variable | Default | Meaning |
|---|---|---|
| `SPARK_MASTER` | `local[4]` | Use `yarn` to submit to the YARN cluster (02 then also starts YARN) |
| `DRIVER_MEMORY` | `6g` | Spark driver heap |
| `MEMORY_CAP_MB` | `8192` | Hard memory ceiling for the whole Spark JVM (systemd cgroup) |
| `RESULTS_DIR` | `results` | Where `3-export/export_results.sh` puts tables and charts |
| `SPARK_SCRATCH` | `data/spark-scratch` | Shuffle/spill directory. Kept off `/tmp`, which is RAM-backed on many systems; emptied after every job |
| `PAGERANK_TOL` / `PAGERANK_RESET` | `1e-4` / `0.15` | Dynamic PageRank convergence tolerance and reset probability |
| `LPA_MAX_STEPS` | `10` | Label-propagation supersteps |
| `SCC_ITERS` | `10` | Strongly-connected-components outer iterations (`0` skips SCC) |
| `LANDMARKS` | `5` | Number of top-PageRank papers used as shortest-path landmarks |

## Outputs

| Location | Contents |
|---|---|
| HDFS `/clean/vertices`, `/clean/edges` | Clean Parquet graph (stage 1) |
| HDFS `/clean/quality_report` | Row counts dropped or flagged at every cleaning step |
| HDFS `/graph/paper_metrics` | Per-paper Parquet: degrees, PageRank and ranks, WCC, SCC, community, triangles, clustering |
| HDFS `/graph/communities` | Per-community Parquet: size, dominant venue, most influential paper |
| HDFS `/graph/summary/*` | Summary CSVs (top papers, hidden gems, venue flows, …) |
| `<results>/tables/*.csv` | The same summaries copied locally |
| `<results>/charts/*.png` | Charts |

## Troubleshooting

| Symptom | Fix |
|---|---|
| `UnsupportedClassVersionError` or Spark refuses to start | Java 8 is active. The scripts pin `/usr/lib/jvm/java-21-openjdk-amd64`; set `CITATION_JAVA_HOME` if yours is elsewhere |
| `HDFS is not reachable` | `./common/start_hadoop.sh`; check `jps` shows NameNode and DataNode |
| `Name node is in safe mode` | Wait, or `hdfs dfsadmin -safemode leave` |
| Download crawls at ~5 KB/s | snap.stanford.edu throttles each connection; the script already uses 32 parallel range requests (`DOWNLOAD_PARALLELISM`) and resumes if re-run |
| `Only N MB RAM available` | Close some applications, or lower `DRIVER_MEMORY` and `MEMORY_CAP_MB` together (cap must exceed heap by ≥ 768 MB) |
| Spark exits with code 137 | It hit `MEMORY_CAP_MB` and the cgroup stopped it (other apps are unaffected). Raise the cap if you have spare RAM, or skip SCC with `SCC_ITERS=0` |
| Spark job hangs with idle cores in local mode | Make sure `SPARK_LOCAL_IP=127.0.0.1` is set (`common/common.sh` does this). Otherwise HDFS-local tasks wait forever for a host with no executor |
