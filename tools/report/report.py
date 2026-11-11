"""Charts of the M6 results in docs/BENCHMARKS.md.

usage: report.py RESULTS OUT

RESULTS is bench-results/m6. Each chart is written to OUT twice, NAME.png on
a light surface and NAME-dark.png on a dark one, from the summaries that
cmd/benchsum and cmd/annbench wrote; nothing is measured here.
"""

import glob
import json
import os
import sys
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FixedLocator, FuncFormatter, NullLocator  # noqa: E402

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", secondary="#52514e", muted="#898781",
                  grid="#e1e0d9", axis="#c3c2b7", series=["#2a78d6", "#eb6834", "#1baf7a"]),
    "dark": dict(surface="#1a1a19", ink="#f0efec", secondary="#c3c2b7", muted="#898781",
                 grid="#2c2c2a", axis="#383835", series=["#3987e5", "#d95926", "#199e70"]),
}


def load(path):
    with open(path) as f:
        return json.load(f)


def style(t):
    plt.rcParams.update({
        "figure.facecolor": t["surface"], "axes.facecolor": t["surface"], "savefig.facecolor": t["surface"],
        "font.family": "sans-serif", "font.size": 9, "text.color": t["ink"],
        "axes.edgecolor": t["axis"], "axes.labelcolor": t["secondary"], "axes.titlesize": 9.5,
        "axes.titlecolor": t["ink"], "axes.titleweight": "bold", "axes.titlelocation": "left",
        "figure.titlesize": 9.5, "figure.titleweight": "bold",
        "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "axes.axisbelow": True,
        "grid.color": t["grid"], "grid.linewidth": 0.75, "grid.linestyle": "-",
        "xtick.color": t["muted"], "ytick.color": t["muted"], "xtick.labelcolor": t["secondary"],
        "ytick.labelcolor": t["secondary"], "legend.frameon": False, "legend.labelcolor": t["secondary"],
        "lines.linewidth": 1.5, "lines.solid_capstyle": "round", "lines.solid_joinstyle": "round",
    })


def dot(t, color):
    """A filled marker with a ring in the surface color."""
    return dict(marker="o", markersize=6, markerfacecolor=color, markeredgecolor=t["surface"], markeredgewidth=1.2)


def ring(t, color):
    return dict(marker="o", markersize=6, markerfacecolor=t["surface"], markeredgecolor=color, markeredgewidth=1.4)


def save(fig, out, name, mode):
    fig.savefig(os.path.join(out, f"{name}{'' if mode == 'light' else '-dark'}.png"), dpi=200, bbox_inches="tight")
    plt.close(fig)


def sweep(res, out, t, mode):
    """Replay with the cache off: p99 and the degraded share of each run by rate."""
    runs = [load(p) for p in glob.glob(f"{res}/sweep-replay-cache-off/*.summary.json")]
    fig, (top, bottom) = plt.subplots(2, 1, figsize=(7, 5), sharex=True, layout="constrained")
    for ax, key, scale, label in ((top, "p99_ms", 1, "p99, ms"), (bottom, "degraded_share", 100, "degraded, % of responses")):
        for clean, kw, name in ((True, dot(t, t["series"][0]), "valid, baselines not flagged"),
                                (False, ring(t, t["series"][0]), "invalid or flagged")):
            sel = [r for r in runs if (r["valid"] and not r["noisy"]) == clean]
            ax.plot([r["rate"] for r in sel], [r[key] * scale for r in sel], linestyle="none", label=name, **kw)
        ax.axvline(550, color=t["muted"], linewidth=0.75)
        ax.set_ylabel(label)
    top.set_ylim(bottom=0)
    bottom.axhline(1, color=t["muted"], linewidth=0.75)
    bottom.text(110, 3, "1% limit", color=t["secondary"])
    top.text(570, 1, "headline 550/s", color=t["secondary"])
    bottom.set_xscale("log")
    rates = sorted({r["rate"] for r in runs if r["rate"] in (100, 200, 400, 800, 1600, 3200)})
    bottom.xaxis.set_major_locator(FixedLocator(rates))
    bottom.xaxis.set_minor_locator(NullLocator())
    bottom.set_xticklabels([f"{r:,}" for r in rates])
    bottom.set_xlabel("requests/s; three runs per rate, one at 4,400")
    top.set_title("Replay, cache off (second VM): the degraded share, not p99, sets the headline")
    bottom.legend(loc="upper left")
    save(fig, out, "sweep", mode)


def faults(res, out, t, mode):
    """Each fault's three runs per second: p99 and the degraded share, with the fault's 60 s shaded."""
    names = ["F1", "F2", "F3", "F4", "F5", "F6"]
    titles = {"F1": "F1 replica killed", "F2": "F2 shard 1 +50 ms", "F3": "F3 Valkey down",
              "F4": "F4 ingest paused", "F5": "F5 replica paused", "F6": "F6 searches hang"}
    fig, axes = plt.subplots(len(names), 2, figsize=(8, 11), sharex=True, layout="constrained")
    for row, f in zip(axes, names):
        for p in sorted(glob.glob(f"{res}/faults/{f}-run*.fault.json")):
            r = load(p)
            color = t["series"][0] if r["pass"] else t["series"][1]
            secs = range(30, len(r["windows"]))
            for ax, key, scale in ((row[0], "p99_ms", 1), (row[1], "degraded_share", 100)):
                ax.plot(secs, [r["windows"][s][key] * scale for s in secs], color=color, linewidth=1)
        for ax in row:
            ax.axvspan(90, 150, color=t["muted"], alpha=0.12, linewidth=0)
        row[0].set_ylabel(titles[f], color=t["ink"])
        row[0].set_ylim(bottom=0)
        row[1].set_ylim(0, max(5, row[1].get_ylim()[1]))
    axes[0][0].set_title("p99 per second, ms")
    axes[0][1].set_title("degraded per second, % of responses")
    for ax in axes[-1]:
        ax.set_xlabel("seconds from the first request")
    fig.suptitle("Faults at 275/s (third VM), three runs each; shaded: the fault, 90 to 150 s", x=0.01, ha="left")
    axes[0][1].plot([], [], color=t["series"][0], label="run that passed")
    axes[0][1].plot([], [], color=t["series"][1], label="run that failed")
    axes[0][1].legend(loc="upper right")
    save(fig, out, "faults", mode)


def swaps(res, out, t, mode):
    """p99 and max over the seconds with a swap and over the others, per run."""
    fig, axes = plt.subplots(1, 2, figsize=(7, 3), layout="constrained")
    groups = (("gc", "GC on"), ("gc-off", "GOGC=off"))
    for ax, key, title in ((axes[0], "p99_ms", "p99 over the windows, ms"), (axes[1], "max_ms", "max over the windows, ms")):
        for i, (prefix, _) in enumerate(groups):
            runs = [load(p) for p in sorted(glob.glob(f"{res}/swap/{prefix}-run*.swap.json"))]
            for j, (w, name) in enumerate((("swap_windows", "seconds with a swap"), ("other_windows", "other seconds"))):
                ax.plot([i + (j - 0.5) * 0.25] * len(runs), [r[w][key] for r in runs], linestyle="none",
                        label=name if i == 0 else None, **dot(t, t["series"][j]))
        ax.set_xticks(range(len(groups)), [g[1] for g in groups])
        ax.set_xlim(-0.6, len(groups) - 0.4)
        ax.set_ylim(bottom=0)
        ax.grid(axis="x", visible=False)
        ax.set_title(title)
    axes[0].legend(loc="lower left")
    fig.suptitle("Bundle swaps at 275/s (third VM): three runs each", x=0.01, ha="left")
    save(fig, out, "swaps", mode)


def kind(res, out, t, mode):
    """p99 per second in kind with SPEC §5's budgets, the phases marked, one panel per run."""
    paths = sorted(glob.glob(f"{res}/kind/run*.phases.json"))
    fig, axes = plt.subplots(len(paths), 1, figsize=(7, 1.8 * len(paths)), sharex=True, sharey=True, layout="constrained")
    for ax, p in zip(axes, paths):
        r, run = load(p), load(p.replace(".phases.json", ".json"))
        start = datetime.fromisoformat(run["start"].replace("Z", "+00:00")).timestamp() * 1000
        warmup = run["warmup_s"]
        ax.plot(range(len(r["windows"])), [w["p99_ms"] for w in r["windows"]], color=t["series"][0])
        with open(p.replace(".phases.json", ".marks")) as f:
            for i, line in enumerate(f):
                name, ms = line.split()
                x = (int(ms) - start) / 1000 - warmup
                ax.axvline(x, color=t["muted"], linewidth=0.75)
                if ax is axes[0]:
                    ax.text(x + 1, 21 - 3 * (i % 2), {"gateways": "gateways roll", "replicas": "replicas roll"}.get(name, name),
                            color=t["secondary"], fontsize=8)
        ax.set_ylabel(f"{r['name']}, p99 ms")
        ax.set_ylim(0, 23)
    axes[0].set_title("kind at 100/s with SPEC §5's budgets (third VM): no error in any phase")
    axes[-1].set_xlabel("seconds after the warmup")
    save(fig, out, "kind", mode)


def ann(res, out, t, mode):
    """recall@100 against p99 at ef 120 to 800, annbench and faiss on the same VM."""
    sets = (("synth-M16", "synth-faiss-M16", "synth, M = 16"), ("glove-M16", "glove-faiss-M16", "glove, M = 16"),
            ("glove-M32", "glove-faiss-M32", "glove, M = 32"), ("glove-M32-2shards", None, "glove, M = 32, two shards"))
    fig, axes = plt.subplots(2, 2, figsize=(7.5, 6), sharey=True, layout="constrained")
    for ax, (ours, faiss, title) in zip(axes.flat, sets):
        pts = [p for p in load(f"{res}/ann/{ours}.json")["points"] if p["k"] == 100]
        ax.axhline(5, color=t["muted"], linewidth=0.75)
        if pts[0]["recall"] < 0.95 < pts[-1]["recall"]:
            ax.axvline(0.95, color=t["muted"], linewidth=0.75)
        ax.plot([p["recall"] for p in pts], [p["latency"]["p99_us"] / 1000 for p in pts],
                color=t["series"][0], label="recserve HNSW", **dot(t, t["series"][0]))
        for p, offset in ((pts[0], (-4, 8)), (pts[-1], (-30, 8))):
            ax.annotate(f"ef {p['ef']}", (p["recall"], p["latency"]["p99_us"] / 1000), textcoords="offset points",
                        xytext=offset, color=t["secondary"], fontsize=8)
        if faiss:
            lat = {(p["ef"], p["k"]): p["p99_us"] for p in load(f"{res}/ann/{faiss}.json")["points"]}
            rec = [p for p in load(f"{res}/ann/{faiss}-recall.json")["points"] if p["k"] == 100 and (p["ef"], 100) in lat]
            ax.plot([p["recall"] for p in rec], [lat[(p["ef"], 100)] / 1000 for p in rec],
                    color=t["series"][1], label="faiss 1.15.1", **dot(t, t["series"][1]))
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(FixedLocator([0.25, 0.5, 1, 2, 5, 10]))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.yaxis.set_minor_locator(NullLocator())
        ax.set_title(title)
        ax.set_xlabel("recall@100")
    for ax in axes[:, 0]:
        ax.set_ylabel("p99, ms")
    axes[0][0].legend(loc="upper left")
    axes[0][1].text(0.715, 5.4, "5 ms", color=t["secondary"], fontsize=8)
    fig.suptitle("ANN at k = 100, one thread (fourth VM): ef 120, 200, 400 and 800; lines at M2's 5 ms and recall 0.95",
                 x=0.01, ha="left")
    save(fig, out, "ann", mode)


def main():
    res, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    for mode, t in THEMES.items():
        style(t)
        for chart in (sweep, faults, swaps, kind, ann):
            chart(res, out, t, mode)


if __name__ == "__main__":
    main()
