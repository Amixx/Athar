"""Benchmark WL refinement backend tradeoffs."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import statistics
import sys
import time
import tracemalloc
from typing import Any, Callable

from athar.determinism import canonical_json
from athar.graph_parser import parse_graph
from athar.wl_refinement import wl_refine_with_scc_fallback

BACKENDS = ["auto", "sha256", "xxh3_64", "blake3", "blake2b_64"]


def _benchmark(
    fn: Callable[[], dict[str, Any]],
    *,
    warmup: int,
    iterations: int,
    label: str,
) -> dict[str, Any]:
    for i in range(warmup):
        print(f"[wl-bench] {label} warmup {i + 1}/{warmup}", file=sys.stderr, flush=True)
        fn()

    times_ms: list[float] = []
    peaks: list[int] = []
    signatures: list[dict[str, Any]] = []
    for i in range(iterations):
        print(f"[wl-bench] {label} iter {i + 1}/{iterations} start", file=sys.stderr, flush=True)
        tracemalloc.start()
        t0 = time.perf_counter()
        signature = fn()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(
            f"[wl-bench] {label} iter {i + 1}/{iterations} done elapsed_ms={elapsed_ms:.2f} peak_bytes={int(peak)}",
            file=sys.stderr,
            flush=True,
        )
        times_ms.append(elapsed_ms)
        peaks.append(int(peak))
        signatures.append(signature)

    stable = all(sig == signatures[0] for sig in signatures[1:]) if signatures else True
    return {
        "samples": {
            "time_ms": [round(v, 3) for v in times_ms],
            "peak_mem_bytes": peaks,
        },
        "summary": {
            "time_ms": {
                "min": round(min(times_ms), 3),
                "max": round(max(times_ms), 3),
                "mean": round(statistics.fmean(times_ms), 3),
            },
            "peak_mem_bytes": {
                "min": min(peaks),
                "max": max(peaks),
                "mean": int(round(statistics.fmean(peaks))),
            },
        },
        "stable_signature": stable,
        "signature": signatures[0] if signatures else {},
    }


def _backend_run(graph: dict, *, backend: str) -> dict[str, Any]:
    colors, class_ids = wl_refine_with_scc_fallback(graph, round_hash=backend)
    return {
        "entity_count": len(colors),
        "class_id_count": len(class_ids),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark WL backend options.")
    parser.add_argument(
        "--graph",
        action="append",
        default=[],
        help="Graph path (IFC). Repeatable. Defaults to data/BasicHouse.ifc and data/AdvancedProject.ifc.",
    )
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=2)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0")
    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1")

    repo_root = Path(__file__).resolve().parents[2]
    graph_paths = [Path(p) for p in args.graph] if args.graph else [
        repo_root / "data" / "BasicHouse.ifc",
        repo_root / "data" / "AdvancedProject.ifc",
    ]

    report: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "warmup": args.warmup,
            "iterations": args.iterations,
            "backends": BACKENDS,
        },
        "graphs": [],
    }

    total_graphs = len(graph_paths)
    for graph_idx, graph_path in enumerate(graph_paths, start=1):
        print(
            f"[wl-bench] graph {graph_idx}/{total_graphs}: parsing {graph_path}",
            file=sys.stderr,
            flush=True,
        )
        parse_started = time.perf_counter()
        graph = parse_graph(str(graph_path), profile="semantic_stable")
        print(
            f"[wl-bench] graph {graph_idx}/{total_graphs}: parsed in {(time.perf_counter() - parse_started) * 1000.0:.1f} ms",
            file=sys.stderr,
            flush=True,
        )
        graph_report: dict[str, Any] = {
            "graph_path": str(graph_path),
            "backends": {},
        }
        for backend_idx, backend in enumerate(BACKENDS, start=1):
            label = f"graph={graph_path.name} backend={backend} ({backend_idx}/{len(BACKENDS)})"
            try:
                print(f"[wl-bench] {label}", file=sys.stderr, flush=True)
                graph_report["backends"][backend] = {
                    "available": True,
                    **_benchmark(
                        lambda backend=backend: _backend_run(graph, backend=backend),
                        warmup=args.warmup,
                        iterations=args.iterations,
                        label=label,
                    ),
                }
            except ValueError as exc:
                graph_report["backends"][backend] = {
                    "available": False,
                    "error": str(exc),
                }
                print(
                    f"[wl-bench] {label} unavailable: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
        report["graphs"].append(graph_report)

    payload = canonical_json(report) + "\n"
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote WL backend benchmark to {out_path}")
    else:
        print(payload, end="")


if __name__ == "__main__":
    main()
