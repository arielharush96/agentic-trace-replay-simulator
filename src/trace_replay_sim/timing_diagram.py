"""Render the per-turn 5-bucket session waterfall from derive.py output.

Consumes ``per_turn.json`` (produced by ``trace_replay_sim.derive``) and renders a
horizontal stacked-bar waterfall: one row per turn, five buckets per row in
execution order A -> C -> E -> D -> B. Turn 0 is heaviest (cold sandbox). Also
renders a cross-layer E2E comparison from the driver summaries.

Usage:
  python3 -m trace_replay_sim.timing_diagram --per-turn results/derive/per_turn.json \
      --out results/diagrams
"""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np


# Buckets in within-turn execution order, with display metadata.
BUCKETS = [
    ("A_premodel_ms", "A. Pre-model (HTTP+ctx)", "#3B82F6"),
    ("C_ttft_ms", "C. Model TTFT", "#F59E0B"),
    ("E_decode_ms", "E. Model decode", "#94A3B8"),
    ("D_response_ms", "D. Response processing", "#10B981"),
    ("B_sandbox_ms", "B. Sandbox init/exec", "#8B5CF6"),
]

# Map single-letter names to bucket keys so callers can say --exclude E.
_LETTER_TO_KEY = {"A": "A_premodel_ms", "B": "B_sandbox_ms", "C": "C_ttft_ms",
                  "D": "D_response_ms", "E": "E_decode_ms"}


def select_buckets(exclude: set[str] | None = None) -> list[tuple[str, str, str]]:
    """Return the bucket list minus any excluded buckets (by key or letter)."""
    if not exclude:
        return list(BUCKETS)
    drop = {_LETTER_TO_KEY.get(e.strip().upper(), e.strip()) for e in exclude}
    return [b for b in BUCKETS if b[0] not in drop]


def load_per_turn(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))


def load_driver_summary(results_dir: Path, layer: str) -> dict[str, Any] | None:
    path = results_dir / layer / "summary.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _val(row: dict, key: str) -> float:
    v = row.get(key)
    return float(v) if v is not None else 0.0


def generate_session_waterfall(rows: list[dict], out_path: Path,
                               title: str = "Per-Turn Session Waterfall",
                               buckets: list[tuple[str, str, str]] | None = None) -> None:
    if not rows:
        print("WARNING: no per-turn rows; skipping waterfall.")
        return
    buckets = buckets if buckets is not None else BUCKETS
    rows = sorted(rows, key=lambda r: r.get("turn_seq", 0))
    n = len(rows)
    fig, ax = plt.subplots(figsize=(13, max(4, n * 0.42)))
    y = np.arange(n)

    left = np.zeros(n)
    for key, _label, color in buckets:
        widths = np.array([_val(r, key) for r in rows])
        ax.barh(y, widths, 0.62, left=left, color=color)
        left += widths

    span = max(left) if max(left) > 0 else 1.0
    for i, r in enumerate(rows):
        total = left[i]
        note = "  (cold)" if r.get("is_cold_sandbox") else ""
        ax.text(total + span * 0.005, i, f"{total:.0f}ms{note}", va="center", fontsize=8)

    ax.set_yticks(y)
    ax.set_yticklabels([f"Turn {r.get('turn_seq')}" for r in rows])
    ax.set_xlabel("Latency (ms)")
    ax.set_title(f"{title} ({n} turns)")
    ax.invert_yaxis()
    ax.legend(handles=[mpatches.Patch(color=color, label=label) for _key, label, color in buckets],
              loc="lower right", fontsize=8)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def generate_avg_breakdown(rows: list[dict], out_path: Path,
                           buckets: list[tuple[str, str, str]] | None = None) -> None:
    """Average per bucket across turns (cold turn-0 shown separately)."""
    if not rows:
        return
    buckets = buckets if buckets is not None else BUCKETS
    labels = [label for _key, label, _color in buckets]
    colors = [c for _k, _l, c in buckets]
    means = []
    for key, _l, _c in buckets:
        vals = [_val(r, key) for r in rows]
        means.append(statistics.mean(vals) if vals else 0.0)
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(labels, means, color=colors)
    ax.set_ylabel("Mean latency (ms)")
    ax.set_title("Mean per-turn bucket breakdown")
    ax.tick_params(axis="x", rotation=20)
    for i, m in enumerate(means):
        ax.text(i, m, f"{m:.0f}", ha="center", va="bottom", fontsize=8)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def generate_layer_comparison(results_dir: Path, out_path: Path) -> None:
    layers = []
    for name in ["mock-direct", "openclaw", "shell"]:
        s = load_driver_summary(results_dir, name)
        if s:
            layers.append({"name": name, "e2e_p50": s.get("e2e_p50_ms") or 0,
                           "e2e_p95": s.get("e2e_p95_ms") or 0,
                           "ttft_p50": s.get("ttft_p50_ms") or 0,
                           "ttft_p95": s.get("ttft_p95_ms") or 0})
    if not layers:
        return
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    names = [layer["name"] for layer in layers]
    x = np.arange(len(names))
    w = 0.35
    for ax, (p50k, p95k, ttl) in zip(axes, [("e2e_p50", "e2e_p95", "End-to-End Latency"),
                                            ("ttft_p50", "ttft_p95", "Time to First Token")]):
        ax.bar(x - w / 2, [layer[p50k] for layer in layers], w, label="P50", color="#3B82F6")
        ax.bar(x + w / 2, [layer[p95k] for layer in layers], w, label="P95", color="#F59E0B")
        ax.set_xticks(x)
        ax.set_xticklabels(names)
        ax.set_ylabel("Latency (ms)")
        ax.set_title(f"{ttl} per Layer")
        ax.legend()
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def generate_all(per_turn_path: Path, results_dir: Path | None, out_dir: Path,
                 exclude: set[str] | None = None) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = load_per_turn(per_turn_path)
    if not rows:
        print("WARNING: no per-turn data found; nothing to render.")
        return
    buckets = select_buckets(exclude)
    if exclude:
        print(f"Excluding buckets: {sorted(exclude)}")
    print(f"Rendering diagrams from {len(rows)} per-turn rows...")
    generate_session_waterfall(rows, out_dir / "session_waterfall.png", buckets=buckets)
    generate_avg_breakdown(rows, out_dir / "bucket_breakdown_avg.png", buckets=buckets)
    if results_dir:
        generate_layer_comparison(results_dir, out_dir / "layer_comparison.png")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Render per-turn 5-bucket waterfall")
    p.add_argument("--per-turn", required=True, help="per_turn.json from trace_replay_sim.derive")
    p.add_argument("--results", default=None, help="results dir with layer subdirs (E2E comparison)")
    p.add_argument("--out", required=True, help="output directory for PNGs")
    args = p.parse_args(argv)
    generate_all(Path(args.per_turn), Path(args.results) if args.results else None, Path(args.out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
