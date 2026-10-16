#!/usr/bin/env python3
"""Draws the report charts from the summary CSVs exported out of HDFS.

Usage: make_charts.py <tables_dir> <charts_dir>

Only reads the small summary tables that the GraphX stage already aggregated in Spark;
no analysis happens here, just plotting.
"""
import csv
import math
import sys
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, LogNorm  # noqa: E402

# Validated reference palette (light mode): categorical slots in fixed order, chrome and ink.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
SURFACE, INK, INK_2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
SEQUENTIAL = LinearSegmentedColormap.from_list("blue", BLUE_RAMP)

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.size": 10,
    "text.color": INK, "axes.labelcolor": INK_2, "axes.titlecolor": INK,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.edgecolor": AXIS,
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "legend.frameon": False, "lines.linewidth": 2,
})


def read(tables, name):
    with open(tables / f"{name}.csv", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def stats(tables):
    return {r["metric"]: r["value"] for r in read(tables, "graph_stats")}


def short(title, width=42):
    return textwrap.shorten(title or "(untitled)", width=width, placeholder="…")


def save(fig, out, name):
    fig.tight_layout()
    fig.savefig(out / f"{name}.png", dpi=200)
    plt.close(fig)
    print(f"  wrote charts/{name}.png")


def degree_distribution(tables, out):
    fig, ax = plt.subplots(figsize=(7, 4.2))
    for i, (kind, label) in enumerate([("in", "Citations received (in-degree)"),
                                        ("out", "References made (out-degree)")]):
        rows = [(int(r[f"{kind}_degree"]), int(r["papers"])) for r in read(tables, f"{kind}_degree_distribution")]
        rows = [(d, c) for d, c in rows if d > 0]
        ax.scatter([d for d, _ in rows], [c for _, c in rows], s=14, color=SERIES[i], label=label,
                   edgecolors=SURFACE, linewidths=0.5)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Degree (log scale)")
    ax.set_ylabel("Number of papers (log scale)")
    ax.set_title("Citations follow a heavy-tailed distribution")
    ax.legend(loc="upper right")
    save(fig, out, "degree_distribution")


def pagerank_vs_citations(tables, out, s):
    rows = read(tables, "pagerank_vs_citations")
    xs = sorted({float(r["log10_citations_plus1"]) for r in rows})
    ys = sorted({float(r["log10_pagerank_x_avg"]) for r in rows})
    grid = [[math.nan] * len(xs) for _ in ys]
    for r in rows:
        grid[ys.index(float(r["log10_pagerank_x_avg"]))][xs.index(float(r["log10_citations_plus1"]))] = int(r["papers"])
    fig, ax = plt.subplots(figsize=(7, 4.6))
    step = 0.1
    mesh = ax.pcolormesh([x for x in xs] + [xs[-1] + step], [y for y in ys] + [ys[-1] + step], grid,
                         cmap=SEQUENTIAL, norm=LogNorm(), shading="flat")
    ax.grid(False)
    cbar = fig.colorbar(mesh, ax=ax, pad=0.02)
    cbar.set_label("Papers in cell (log scale)", color=INK_2)
    cbar.outline.set_visible(False)
    # Bins are log10(citations + 1), so place round citation counts at their binned position.
    counts = [c for c in (0, 10, 100, 1000, 10000) if math.log10(c + 1) <= max(xs) + step]
    ax.set_xticks([math.log10(c + 1) for c in counts], [f"{c:,}" for c in counts])
    yt = range(math.floor(min(ys)), math.ceil(max(ys)) + 1)
    ax.set_yticks(list(yt), [f"{10.0 ** t:g}×" for t in yt])
    ax.set_xlabel("Citations received")
    ax.set_ylabel("PageRank relative to the average paper")
    ax.set_title("PageRank mostly agrees with citation counts", pad=20)
    ax.text(0, 1.015, f"Spearman ρ = {float(s['pagerank.spearman_vs_citations']):.2f} over all "
            f"{int(s['graph.vertices']):,} papers", transform=ax.transAxes, fontsize=9, color=INK_2, va="bottom")
    save(fig, out, "pagerank_vs_citations")


def by_year(tables, out):
    rows = [r for r in read(tables, "by_year") if int(r["papers"]) >= 20]
    years = [int(r["year"]) for r in rows]
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(7, 5.2), sharex=True)
    top.bar(years, [int(r["papers"]) for r in rows], color=SERIES[0], width=0.8)
    top.set_ylabel("Papers")
    top.set_title("Papers per year and their average influence")
    top.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    bottom.plot(years, [float(r["mean_pagerank_x_avg"]) for r in rows], color=SERIES[0], marker="o", markersize=4)
    bottom.axhline(1.0, color=MUTED, linewidth=1, linestyle="--")
    bottom.text(years[0], 1.0, " average paper", color=MUTED, va="bottom", fontsize=9)
    bottom.set_ylabel("Mean PageRank ÷ average")
    bottom.set_xlabel("Publication year (years with ≥ 20 papers)")
    save(fig, out, "by_year")


def category_flows(tables, out, top_n=15):
    cats = [r["category"] for r in read(tables, "categories")][:top_n]
    flows = {(r["citing_category"], r["cited_category"]): int(r["citations"]) for r in read(tables, "category_flows")}
    totals = {c: sum(v for (src, _), v in flows.items() if src == c) for c in cats}
    grid = [[flows.get((src, dst), 0) / totals[src] if totals[src] else 0 for dst in cats] for src in cats]
    fig, ax = plt.subplots(figsize=(8, 7))
    im = ax.imshow(grid, cmap=SEQUENTIAL, vmin=0, vmax=1)
    ax.grid(False)
    ax.set_xticks(range(len(cats)), cats, rotation=60, ha="right")
    ax.set_yticks(range(len(cats)), cats)
    ax.tick_params(colors=INK_2)
    for i, row in enumerate(grid):
        for j, v in enumerate(row):
            if v >= 0.05:
                ax.text(j, i, f"{v:.0%}", ha="center", va="center", fontsize=7,
                        color="white" if v >= 0.45 else INK)
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.03, format=matplotlib.ticker.PercentFormatter(1.0))
    cbar.outline.set_visible(False)
    cbar.set_label("Share of the citing venue's citations", color=INK_2)
    ax.set_xlabel("Cited venue")
    ax.set_ylabel("Citing venue")
    ax.set_title(f"Where each venue's citations go ({top_n} largest venues)")
    save(fig, out, "category_flows")


def community_validation(tables, out, s):
    metrics = [("NMI", "nmi"), ("ARI", "ari"), ("Purity", "purity")]
    lpa = [float(s[f"lpa.{k}_vs_category"]) for _, k in metrics]
    base = [float(s[f"lpa.baseline_{k}"]) for _, k in metrics]
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    width = 0.36
    xs = range(len(metrics))
    for i, (vals, label) in enumerate([(lpa, "Label Propagation communities"), (base, "Random baseline (same sizes)")]):
        bars = ax.bar([x + (i - 0.5) * (width + 0.02) for x in xs], vals, width, color=SERIES[i], label=label)
        ax.bar_label(bars, labels=[f"{abs(v) if abs(v) < 0.005 else v:.2f}" for v in vals], padding=3,
                     color=INK_2, fontsize=9)
    ax.set_xticks(list(xs), [m for m, _ in metrics])
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("Agreement with publication venues")
    ax.set_title("Citation communities vs. publication venues")
    ax.legend(loc="upper left")
    save(fig, out, "community_validation")


def landmark_reach(tables, out, s, max_hops=10):
    n = int(s["graph.vertices"])
    rows = read(tables, "landmark_reach")
    order = sorted({(float(r["pagerank_rank"]), r["landmark"]) for r in rows})
    fig, ax = plt.subplots(figsize=(7.5, 4.4))
    for i, (rank, lm) in enumerate(order):
        pts = sorted((int(r["hops"]), int(r["cumulative_papers"]) / n * 100, r["title"], r["year"])
                     for r in rows if r["landmark"] == lm and int(r["hops"]) <= max_hops)
        title, year = pts[0][2], pts[0][3]
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=SERIES[i], marker="o", markersize=4,
                label=f"{short(title, 46)} ({year}, PageRank #{rank:.0f})")
    ax.set_xlabel("Citation hops from the landmark paper")
    ax.set_ylabel("% of all papers reached (cumulative)")
    ax.yaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(100))
    ax.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(integer=True))
    ax.set_title("How far the five most influential papers' influence spreads")
    ax.legend(loc="lower right", fontsize=8)
    save(fig, out, "landmark_reach")


def main():
    tables, out = Path(sys.argv[1]), Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    s = stats(tables)
    degree_distribution(tables, out)
    pagerank_vs_citations(tables, out, s)
    by_year(tables, out)
    category_flows(tables, out)
    community_validation(tables, out, s)
    landmark_reach(tables, out, s)


if __name__ == "__main__":
    main()
