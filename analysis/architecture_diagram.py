#!/usr/bin/env python3
"""Draws docs/architecture.png and .svg: the pipeline's compute stages above the HDFS layer they
read from and write to. Usage: architecture_diagram.py <out_dir>"""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

SURFACE, INK, INK_2, MUTED = "#fcfcfb", "#0b0b0b", "#52514e", "#898781"
# Tool colours: categorical slots 1-3 of the validated palette, as fill washes + solid borders.
HDFS, SPARK, GRAPHX, OTHER = "#2a78d6", "#eb6834", "#1baf7a", "#898781"
WASH = {HDFS: "#e3eefb", SPARK: "#fce6dc", GRAPHX: "#dcf3ea", OTHER: "#f0efec"}


def box(ax, x, y, w, h, color, title, body):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0,rounding_size=1.2",
                                facecolor=WASH[color], edgecolor=color, linewidth=1.6))
    ax.text(x + w / 2, y + h - 2.0, title, ha="center", va="top", fontsize=10.5, weight="bold", color=INK)
    ax.text(x + w / 2, y + h - 5.4, body, ha="center", va="top", fontsize=8, color=INK_2, linespacing=1.45)


def arrow(ax, start, end, label=None, label_side="right"):
    ax.add_patch(FancyArrowPatch(start, end, arrowstyle="-|>", mutation_scale=13, linewidth=1.4, color=INK_2,
                                 shrinkA=0, shrinkB=0))
    if label:
        dx = 0.9 if label_side == "right" else -0.9
        ax.text((start[0] + end[0]) / 2 + dx, (start[1] + end[1]) / 2, label, fontsize=7.5, color=MUTED,
                ha="left" if label_side == "right" else "right", va="center", style="italic")


def main():
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 5.6))
    fig.patch.set_facecolor(SURFACE)
    ax.set_xlim(-1, 101)
    ax.set_ylim(-4.5, 47)
    ax.axis("off")

    ax.text(0, 46, "Academic Citation Network Analyzer — pipeline architecture", fontsize=13, weight="bold",
            color=INK, va="top")
    ax.text(0, 43.2, "Three course tools: HDFS (storage)  →  Spark (ETL)  →  Spark GraphX (graph analytics). "
                     "Every hand-off goes through HDFS.", fontsize=9, color=INK_2, va="top")

    # Compute row
    cy, ch = 21, 16.5
    box(ax, 0, cy, 18, ch, OTHER, "Ingest", "ogbn-arxiv (OGB / SNAP)\n6 raw files, 150 MB gz\n"
                                            "parallel ranged download\ngunzip | hdfs dfs -put")
    box(ax, 24, cy, 21, ch, SPARK, "Stage 1 · Spark ETL", "CitationETL.scala\nparse 3 file layouts, join 5 sources\n"
                                                          "drop malformed / self / dup / dangling\n"
                                                          "infer citation direction from years\n"
                                                          "flag citations to newer papers")
    box(ax, 51, cy, 24, ch, GRAPHX, "Stage 2 · Spark GraphX", "CitationGraphAnalytics.scala\n"
                                                              "PageRank · Connected Components\n"
                                                              "Strongly Connected Components\n"
                                                              "Label Propagation · Triangle Count\n"
                                                              "ShortestPaths · triplet field flows")
    box(ax, 81, cy, 19, ch, OTHER, "Results", "hdfs dfs -getmerge\nresults/tables (CSV)\n"
                                              "results/charts (PNG)\nquery.sh → Spark SQL")

    # HDFS layer
    ax.add_patch(FancyBboxPatch((4, 0), 92, 15.5, boxstyle="round,pad=0,rounding_size=1.5",
                                facecolor="#f3f8fe", edgecolor=HDFS, linewidth=1.2, linestyle=(0, (4, 3))))
    ax.text(50, -1.2, "HDFS layer  ·  hdfs://localhost:9000/user/<you>/citation", fontsize=9, weight="bold",
            color=HDFS, va="top", ha="center")
    sy, sh = 1.5, 10
    box(ax, 7, sy, 22, sh, HDFS, "/raw", "plain text, splittable\nedges · years · labels\nid maps · titles")
    box(ax, 36, sy, 22, sh, HDFS, "/clean", "Parquet\nvertices · edges\nquality_report (CSV)")
    box(ax, 65, sy, 26, sh, HDFS, "/graph", "Parquet: paper_metrics, communities\n"
                                             "CSV: 16 summary tables\n(top papers, flows, reach …)")

    top, bottom = cy, sy + sh
    arrow(ax, (9, top), (14, bottom), "write")
    arrow(ax, (25, bottom), (30, top), "read")
    arrow(ax, (40, top), (45, bottom), "write Parquet")
    arrow(ax, (54, bottom), (59, top), "read")
    arrow(ax, (70, top), (75, bottom), "write")
    arrow(ax, (86, bottom), (90, top), "getmerge")

    # Legend
    for i, (color, name) in enumerate([(HDFS, "HDFS"), (SPARK, "Spark (core + SQL)"), (GRAPHX, "Spark GraphX"),
                                       (OTHER, "Shell / Python glue")]):
        x = i * 17
        ax.add_patch(FancyBboxPatch((x, 38.7), 1.8, 1.8, boxstyle="round,pad=0,rounding_size=0.4",
                                    facecolor=WASH[color], edgecolor=color, linewidth=1.4))
        ax.text(x + 2.6, 39.6, name, fontsize=8.5, color=INK_2, va="center")

    fig.tight_layout()
    for ext in ("png", "svg"):
        fig.savefig(out / f"architecture.{ext}", dpi=200, facecolor=SURFACE)
    print(f"  wrote {out}/architecture.png and .svg")


if __name__ == "__main__":
    main()
