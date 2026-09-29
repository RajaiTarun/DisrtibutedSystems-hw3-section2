#!/usr/bin/env python3
"""
Makes the plots and summary tables for Q1 (MapReduce vs MPI) from results/bench_results.csv

Usage (from inside q1_mapreduce):
    python3 plot_results.py

Output:
    plots/runtime_vs_workers.png    runtime vs number of workers, one panel per input size
    plots/runtime_vs_size.png       runtime and throughput vs input size (4 workers each)
    plots/speedup.png               speedup vs workers for mapreduce and mpi (vs their own 1 worker run)
    plots/mr_stage_breakdown.png    where mapreduce spends its time (50M records)
    plots/combiner_effect.png       mapreduce with vs without the combiner (10M records)
    plots/shuffle_lines.png         how many lines reach the shuffle vs number of mappers
    plots/memory.png                peak memory per process (from bench_extra.sh, PART=memory)
    plots/map_phase_stages.png      mapper / sort / combiner run separately (bench_extra.sh, PART=stages)
    plots/multinode.png             mapreduce on 1 node vs several nodes (bench_extra.sh, PART=multinode)
    results/summary.md              the same numbers as markdown tables (for the report)
"""

import os
import pandas as pd
import matplotlib

matplotlib.use("Agg")  # no window, just write png files
import matplotlib.pyplot as plt

CSV = "results/bench_results.csv"
PLOTS = "plots"
SUMMARY = "results/summary.md"

# ---- colors (validated palette: categorical slots for implementations, one blue ramp for sizes) ----
COLOR = {
    "mpi": "#2a78d6",                   # blue
    "mapreduce": "#eb6834",             # orange
    "mapreduce_nocombiner": "#1baf7a",  # aqua
}
STAGE_COLOR = {"split": "#2a78d6", "map": "#eb6834", "shuffle": "#1baf7a", "reduce": "#eda100"}
SIZE_COLOR = ["#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]  # light -> dark = small -> big input
REFERENCE_GRAY = "#898781"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SURFACE = "#fcfcfb"

NAME = {
    "sequential": "Sequential (HW2)",
    "mpi": "MPI (HW2)",
    "mapreduce": "MapReduce",
    "mapreduce_nocombiner": "MapReduce, no combiner",
}

plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "axes.edgecolor": AXIS,
    "axes.labelcolor": INK_2,
    "axes.titlecolor": INK,
    # big fonts, because the plots are scaled down to half a page in the report
    "axes.titlesize": 13,
    "axes.labelsize": 12,
    "xtick.labelsize": 11,
    "ytick.labelsize": 11,
    "legend.fontsize": 11,
    "axes.grid": True,
    "axes.axisbelow": True,  # gridlines behind the bars, not on top of them
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.color": INK_2,
    "ytick.color": INK_2,
    "legend.frameon": False,
    "font.size": 11,
    "savefig.dpi": 150,
    "savefig.bbox": "tight",
})


def millions(n):
    return f"{n // 1_000_000}M"


def style_workers_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8])
    ax.set_xticklabels(["1", "2", "4", "8"])
    ax.minorticks_off()


def save(fig, name):
    path = os.path.join(PLOTS, name)
    fig.savefig(path)
    plt.close(fig)
    print("wrote", path)


def extra_plots(bench, out):
    """plots and tables for the results of bench_extra.sh (memory, separate stages, multinode)"""

    # ---------------- 7. peak memory per process (50M records) ----------------
    if os.path.exists("results/extra_memory.csv"):
        mem = pd.read_csv("results/extra_memory.csv")
        n = int(mem["N"].iloc[0])

        # one bar per kind of process: the biggest value over all tasks of that kind
        def peak(impl, workers, prefix):
            r = mem[(mem["impl"] == impl) & (mem["workers"] == workers) & mem["process"].str.startswith(prefix)]
            return float(r["max_rss_mb"].max())

        bars = [
            ("Sequential", peak("sequential", 1, "sequential"), REFERENCE_GRAY),
            ("MPI master (4 workers)", peak("mpi", 4, "master"), COLOR["mpi"]),
            ("MPI worker (4 workers)", peak("mpi", 4, "worker"), COLOR["mpi"]),
            ("MPI master (8 workers)", peak("mpi", 8, "master"), COLOR["mpi"]),
            ("MPI worker (8 workers)", peak("mpi", 8, "worker"), COLOR["mpi"]),
            ("MR mapper", peak("mapreduce", 8, "mapper"), COLOR["mapreduce"]),
            ("MR sort (in the pipe)", peak("mapreduce", 8, "sort"), COLOR["mapreduce"]),
            ("MR combiner", peak("mapreduce", 8, "combiner"), COLOR["mapreduce"]),
            ("MR shuffle (sort -m)", peak("mapreduce", 8, "shuffle"), COLOR["mapreduce"]),
            ("MR reducer", peak("mapreduce", 8, "reducer"), COLOR["mapreduce"]),
        ]
        fig, ax = plt.subplots(figsize=(7.5, 4.2))
        labels = [b[0] for b in bars][::-1]
        values = [b[1] for b in bars][::-1]
        colors = [b[2] for b in bars][::-1]
        rects = ax.barh(labels, values, color=colors, height=0.6, edgecolor=SURFACE, linewidth=1.5)
        ax.set_xscale("log")
        ax.bar_label(rects, labels=[f"{v:,.1f} MB" for v in values], padding=4, color=INK_2, fontsize=10)
        ax.set_xlim(1, max(values) * 8)
        ax.set_xlabel("Peak memory per process (MB, log scale)")
        ax.set_title(f"Peak memory per process (N = {millions(n)} records)")
        ax.grid(axis="y", visible=False)
        save(fig, "memory.png")

        # table: per process kind, and the total over all processes of one run
        out += [f"## Peak memory (N = {millions(n)})", "",
                "| Implementation | Workers | Process | Peak memory per process (MB) | Processes |",
                "|---|---|---|---|---|"]
        for (impl, workers), group in mem.groupby(["impl", "workers"], sort=False):
            kinds = group["process"].str.replace(r"_(rank)?\d+$", "", regex=True)
            for kind, g in group.groupby(kinds, sort=False):
                out.append(f"| {impl} | {workers} | {kind} | {g['max_rss_mb'].max():,.1f} | {len(g)} |")
        out += ["", "| Implementation | Workers | Total memory of all processes (MB) |", "|---|---|---|"]
        for (impl, workers), group in mem.groupby(["impl", "workers"], sort=False):
            out.append(f"| {impl} | {workers} | {group['max_rss_mb'].sum():,.1f} |")
        out.append("")

    # ---------------- 8. map phase split into its stages (10M records, 4 mappers) ----------------
    if os.path.exists("results/extra_stages.csv"):
        st = pd.read_csv("results/extra_stages.csv")
        n = int(st["N"].iloc[0])
        p = int(st["mappers"].iloc[0])
        order = ["split", "mapper", "sort", "combiner", "shuffle", "reduce"]
        times = []
        for stage in order:
            name = "reducer" if stage == "reduce" else stage
            r = st[st["stage"] == name]
            whole = r[r["task"] == "all"]
            # the stages that ran with srun have an "all" row (wall time), the others have only one task
            times.append(float(whole["elapsed_s"].iloc[0]) if len(whole) else float(r["elapsed_s"].max()))
        fig, ax = plt.subplots(figsize=(6.5, 3.6))
        rects = ax.bar([s.capitalize() for s in order], times, width=0.55, color=COLOR["mapreduce"],
                       edgecolor=SURFACE, linewidth=1.5)
        ax.bar_label(rects, fmt="%.1fs", padding=2, color=INK_2, fontsize=10)
        ax.set_ylabel("Time (s)")
        ax.set_title(f"MapReduce stages run separately (N = {millions(n)}, {p} mappers)")
        ax.set_ylim(0, max(times) * 1.15)
        ax.grid(axis="x", visible=False)
        save(fig, "map_phase_stages.png")

        mapper_bytes = None
        if os.path.exists("results/extra_stages_mapper_bytes.txt"):
            mapper_bytes = int(open("results/extra_stages_mapper_bytes.txt").read().strip())
        out += [f"## MapReduce stages run separately (N = {millions(n)}, {p} mappers)", "",
                "| Stage | Time (s) | Peak memory per task (MB) |", "|---|---|---|"]
        for stage, t in zip(order, times):
            name = "reducer" if stage == "reduce" else stage
            r = st[(st["stage"] == name) & (st["task"] != "all")]
            m = f"{r['max_rss_mb'].max():,.1f}" if len(r) else "-"
            out.append(f"| {stage} | {t:.2f} | {m} |")
        if mapper_bytes:
            out += ["", f"Mapper output for {millions(n)} records: {mapper_bytes / 1e9:.2f} GB "
                        f"({mapper_bytes / n:.0f} bytes per input record)."]
        out.append("")

    # ---------------- 9. 1 node vs several nodes (50M records, 8 mappers) ----------------
    if os.path.exists("results/extra_multinode.csv"):
        mn = pd.read_csv("results/extra_multinode.csv")
        row = mn.iloc[0]
        n = int(row["N"])
        p = int(row["mappers"])
        one = bench[(bench["impl"] == "mapreduce") & (bench["N"] == n) & (bench["workers"] == p)].iloc[0]
        runs = [("1 node", one), (f"{int(row['nodes'])} nodes", row)]
        fig, ax = plt.subplots(figsize=(5.5, 3.8))
        bottom = [0.0, 0.0]
        labels = [r[0] for r in runs]
        for stage in ["split", "map", "shuffle", "reduce"]:
            vals = [float(r[1][f"{stage}_s"]) for r in runs]
            ax.bar(labels, vals, bottom=bottom, width=0.5, color=STAGE_COLOR[stage],
                   edgecolor=SURFACE, linewidth=1.5, label=stage.capitalize())
            bottom = [b + v for b, v in zip(bottom, vals)]
        for x, r in zip(labels, runs):
            total = float(r[1]["total_s"])
            ax.annotate(f"{total:.0f}s", (x, total), textcoords="offset points", xytext=(0, 4),
                        ha="center", color=INK)
        ax.set_ylabel("Time (s)")
        ax.set_title(f"MapReduce on 1 node vs {int(row['nodes'])} nodes (N = {millions(n)}, {p} mappers)")
        ax.set_ylim(0, max(bottom) * 1.15)
        ax.grid(axis="x", visible=False)
        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=4, fontsize=10)
        save(fig, "multinode.png")

        out += [f"## MapReduce on 1 node vs {int(row['nodes'])} nodes (N = {millions(n)}, {p} mappers)", "",
                "| Nodes | Split | Map | Shuffle | Reduce | Total | Correct |", "|---|---|---|---|---|---|---|"]
        for label, r in runs:
            out.append(f"| {label} | {float(r['split_s']):.2f} | {float(r['map_s']):.2f} | {float(r['shuffle_s']):.2f} | "
                       f"{float(r['reduce_s']):.2f} | {float(r['total_s']):.2f} | {r['correct']} |")
        out.append("")


def main():
    os.makedirs(PLOTS, exist_ok=True)
    df = pd.read_csv(CSV)
    sizes = sorted(df["N"].unique())
    workers = [1, 2, 4, 8]

    def rows(impl, n=None):
        r = df[df["impl"] == impl]
        if n is not None:
            r = r[r["N"] == n]
        return r.sort_values("workers")

    def seq_time(n):
        return float(rows("sequential", n)["total_s"].iloc[0])

    # total time of one implementation for one size and one number of workers
    def time_at(impl, n, w):
        r = rows(impl, n)
        return float(r[r["workers"] == w]["total_s"].iloc[0])

    # ---------------- 1. runtime vs workers, one panel per size ----------------
    fig, axes = plt.subplots(1, len(sizes), figsize=(3.2 * len(sizes), 3.2))
    for ax, n in zip(axes, sizes):
        for impl in ["mpi", "mapreduce"]:
            r = rows(impl, n)
            ax.plot(r["workers"], r["total_s"], color=COLOR[impl], linewidth=2,
                    marker="o", markersize=5, label=NAME[impl])
        ax.axhline(seq_time(n), color=REFERENCE_GRAY, linewidth=1.5, linestyle="--",
                   label=NAME["sequential"])
        style_workers_axis(ax)
        ax.set_ylim(bottom=0)
        ax.set_title(f"N = {millions(n)} records")
    axes[0].set_ylabel("Total time (s)")
    fig.supxlabel("Workers (MPI) / mappers (MapReduce)", color=INK_2, fontsize=12, y=-0.04)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, bbox_to_anchor=(0.5, 1.08))
    fig.suptitle("Runtime vs number of workers", y=1.15, fontsize=14, color=INK)
    save(fig, "runtime_vs_workers.png")

    # ---------------- 2. runtime and throughput vs input size (4 workers) ----------------
    W = 4
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 3.6))
    xs = [n / 1e6 for n in sizes]
    series = [
        ("sequential", [seq_time(n) for n in sizes], REFERENCE_GRAY, "--"),
        ("mpi", [time_at("mpi", n, W) for n in sizes], COLOR["mpi"], "-"),
        ("mapreduce", [time_at("mapreduce", n, W) for n in sizes], COLOR["mapreduce"], "-"),
    ]
    for impl, times, color, style in series:
        label = NAME[impl] if impl == "sequential" else f"{NAME[impl]}, {W} workers"
        ax1.plot(xs, times, color=color, linewidth=2, linestyle=style, marker="o", markersize=5, label=label)
        # direct label at the end of the line
        ax1.annotate(f"{times[-1]:.0f}s", (xs[-1], times[-1]), textcoords="offset points",
                     xytext=(6, 0), va="center", color=INK_2)
        throughput = [n / t / 1e6 for n, t in zip(sizes, times)]
        ax2.plot(xs, throughput, color=color, linewidth=2, linestyle=style, marker="o", markersize=5, label=label)
    ax1.set_title("Total time vs input size")
    ax1.set_xlabel("Input size (million records)")
    ax1.set_ylabel("Total time (s)")
    ax1.set_ylim(bottom=0)
    ax1.set_xlim(right=max(xs) * 1.12)
    ax2.set_title("Throughput vs input size")
    ax2.set_xlabel("Input size (million records)")
    ax2.set_ylabel("Million records / s")
    ax2.set_ylim(bottom=0)
    ax1.legend(loc="upper left")
    save(fig, "runtime_vs_size.png")

    # ---------------- 3. speedup vs workers (each vs its own 1-worker run) ----------------
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), sharey=True)
    for ax, impl in zip(axes, ["mapreduce", "mpi"]):
        for color, n in zip(SIZE_COLOR, sizes):
            r = rows(impl, n)
            base = time_at(impl, n, 1)
            ax.plot(r["workers"], base / r["total_s"], color=color, linewidth=2, marker="o",
                    markersize=5, label=f"N = {millions(n)}")
        ax.plot(workers, workers, color=REFERENCE_GRAY, linewidth=1.5, linestyle="--", label="Ideal")
        style_workers_axis(ax)
        ax.set_ylim(0, 8.5)
        ax.set_title(f"{NAME[impl]}: speedup vs its own 1-worker run")
        ax.set_xlabel("Workers / mappers")
    axes[0].set_ylabel("Speedup")
    axes[0].legend(loc="upper left")
    save(fig, "speedup.png")

    # ---------------- 4. mapreduce stage breakdown for the biggest size ----------------
    n = sizes[-1]
    r = rows("mapreduce", n)
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    labels = [str(w) for w in r["workers"]]
    bottom = [0.0] * len(r)
    for stage in ["split", "map", "shuffle", "reduce"]:
        vals = list(r[f"{stage}_s"])
        ax.bar(labels, vals, bottom=bottom, width=0.55, color=STAGE_COLOR[stage],
               edgecolor=SURFACE, linewidth=1.5, label=stage.capitalize())
        bottom = [b + v for b, v in zip(bottom, vals)]
    for x, total in zip(labels, r["total_s"]):
        ax.annotate(f"{total:.0f}s", (x, total), textcoords="offset points", xytext=(0, 4),
                    ha="center", color=INK)
    ax.set_title(f"Where MapReduce spends its time (N = {millions(n)})")
    ax.set_ylim(0, max(r["total_s"]) * 1.12)
    ax.set_xlabel("Number of mappers")
    ax.set_ylabel("Time (s)")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper right")
    save(fig, "mr_stage_breakdown.png")

    # ---------------- 5. combiner vs no combiner ----------------
    nc_sizes = sorted(df[df["impl"] == "mapreduce_nocombiner"]["N"].unique())
    n = nc_sizes[-1]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 3.6))
    width = 0.38
    xpos = list(range(len(workers)))
    for offset, impl in [(-width / 2, "mapreduce"), (width / 2, "mapreduce_nocombiner")]:
        r = rows(impl, n)
        pos = [x + offset for x in xpos]
        label = "With combiner" if impl == "mapreduce" else "Without combiner"
        bars = ax1.bar(pos, r["total_s"], width=width, color=COLOR[impl], edgecolor=SURFACE,
                       linewidth=1.5, label=label)
        ax1.bar_label(bars, fmt="%.0f", padding=2, color=INK_2, fontsize=10)
        bars = ax2.bar(pos, r["shuffle_lines"] / 1e6, width=width, color=COLOR[impl],
                       edgecolor=SURFACE, linewidth=1.5, label=label)
        ax2.bar_label(bars, fmt="%.1f", padding=2, color=INK_2, fontsize=10)
    for ax in (ax1, ax2):
        ax.set_xticks(xpos)
        ax.set_xticklabels([str(w) for w in workers])
        ax.set_xlabel("Number of mappers")
        ax.grid(axis="x", visible=False)
    ax1.set_title(f"Total time (N = {millions(n)})")
    ax1.set_ylabel("Time (s)")
    ax2.set_title(f"Lines sent to the shuffle (N = {millions(n)})")
    ax2.set_ylabel("Million lines")
    ax1.legend(loc="upper right")
    save(fig, "combiner_effect.png")

    # ---------------- 6. shuffle lines vs mappers ----------------
    fig, ax = plt.subplots(figsize=(6.5, 3.8))
    for color, n in zip(SIZE_COLOR, sizes):
        r = rows("mapreduce", n)
        ax.plot(r["workers"], r["shuffle_lines"] / 1e6, color=color, linewidth=2, marker="o",
                markersize=5, label=f"N = {millions(n)}")
    style_workers_axis(ax)
    ax.set_ylim(bottom=0)
    ax.set_title("Lines reaching the shuffle (with combiner)")
    ax.set_xlabel("Number of mappers")
    ax.set_ylabel("Million lines")
    ax.legend(loc="upper left")
    save(fig, "shuffle_lines.png")

    # ---------------- summary tables (markdown) ----------------
    out = ["# Q1 benchmark summary", "",
           f"Generated by `plot_results.py` from `{CSV}`. Times are in seconds.", ""]

    out += ["## Total time", "",
            "| N | Sequential | " + " | ".join(f"MPI {w}" for w in workers) + " | "
            + " | ".join(f"MR {w}" for w in workers) + " |",
            "|---" * (1 + 1 + 2 * len(workers)) + "|"]
    for n in sizes:
        mpi = [f"{t:.2f}" for t in rows("mpi", n)["total_s"]]
        mr = [f"{t:.2f}" for t in rows("mapreduce", n)["total_s"]]
        out.append(f"| {millions(n)} | {seq_time(n):.2f} | " + " | ".join(mpi) + " | " + " | ".join(mr) + " |")
    out.append("")

    out += ["## MapReduce stage breakdown", "",
            "| N | Mappers | Split | Map (map+sort+combine) | Shuffle | Reduce | Total | Shuffle lines | Correct |",
            "|---|---|---|---|---|---|---|---|---|"]
    for n in sizes:
        for _, x in rows("mapreduce", n).iterrows():
            out.append(f"| {millions(n)} | {x.workers} | {x.split_s:.2f} | {x.map_s:.2f} | {x.shuffle_s:.2f} | "
                       f"{x.reduce_s:.2f} | {x.total_s:.2f} | {int(x.shuffle_lines):,} | {x.correct} |")
    out.append("")

    out += ["## Combiner vs no combiner", "",
            "| N | Mappers | Time with | Time without | Shuffle lines with | Shuffle lines without |",
            "|---|---|---|---|---|---|"]
    for n in nc_sizes:
        a = rows("mapreduce", n).set_index("workers")
        b = rows("mapreduce_nocombiner", n).set_index("workers")
        for w in workers:
            out.append(f"| {millions(n)} | {w} | {a.loc[w, 'total_s']:.2f} | {b.loc[w, 'total_s']:.2f} | "
                       f"{int(a.loc[w, 'shuffle_lines']):,} | {int(b.loc[w, 'shuffle_lines']):,} |")
    out.append("")

    out += ["## Correctness of every run (vs sequential)", "",
            "| Result | Runs |", "|---|---|"]
    for result, count in df["correct"].value_counts().items():
        out.append(f"| {result} | {count} |")
    out.append("")

    # plots + tables from bench_extra.sh (only if those result files exist)
    extra_plots(df, out)

    with open(SUMMARY, "w") as f:
        f.write("\n".join(out))
    print("wrote", SUMMARY)


if __name__ == "__main__":
    main()
