"""Benchmark rooted-owner projection in memory vs disk-spill modes."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from pathlib import Path
import statistics
import sys
import time
import tracemalloc
from typing import Any, Callable

from athar.determinism import canonical_json
from athar.diff_engine import diff_graphs
from athar.diff_engine_markers import OWNER_INDEX_DISK_THRESHOLD_ENV
from athar.graph_parser import parse_graph


@dataclass(frozen=True)
class Case:
    name: str
    old_path: Path
    new_path: Path


def _parse_case(raw: str) -> Case:
    parts = raw.split(":", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid --case value {raw!r}; expected NAME:OLD_PATH:NEW_PATH")
    name, old_path, new_path = parts
    return Case(name=name, old_path=Path(old_path), new_path=Path(new_path))


def _default_case(repo_root: Path) -> Case:
    return Case(
        name="house_v1_v2",
        old_path=repo_root / "tests" / "fixtures" / "house_v1.ifc",
        new_path=repo_root / "tests" / "fixtures" / "house_v2.ifc",
    )


def _with_owner_threshold(threshold: int, fn: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    previous = os.environ.get(OWNER_INDEX_DISK_THRESHOLD_ENV)
    os.environ[OWNER_INDEX_DISK_THRESHOLD_ENV] = str(max(0, threshold))
    try:
        return fn()
    finally:
        if previous is None:
            os.environ.pop(OWNER_INDEX_DISK_THRESHOLD_ENV, None)
        else:
            os.environ[OWNER_INDEX_DISK_THRESHOLD_ENV] = previous


def _result_signature(result: dict[str, Any]) -> dict[str, Any]:
    op_counts: dict[str, int] = {}
    for change in result.get("base_changes", []):
        op = change.get("op")
        if isinstance(op, str):
            op_counts[op] = op_counts.get(op, 0) + 1
    payload = canonical_json(result)
    return {
        "base_change_count": len(result.get("base_changes", [])),
        "derived_marker_count": len(result.get("derived_markers", [])),
        "op_counts": {k: op_counts[k] for k in sorted(op_counts)},
        "result_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
    }


def _benchmark(
    fn: Callable[[], dict[str, Any]],
    *,
    warmup: int,
    iterations: int,
    label: str,
) -> dict[str, Any]:
    for i in range(warmup):
        print(f"[owner-bench] {label} warmup {i + 1}/{warmup}", file=sys.stderr, flush=True)
        fn()

    times_ms: list[float] = []
    peaks: list[int] = []
    signatures: list[dict[str, Any]] = []
    for i in range(iterations):
        print(f"[owner-bench] {label} iter {i + 1}/{iterations} start", file=sys.stderr, flush=True)
        tracemalloc.start()
        started = time.perf_counter()
        signature = fn()
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(
            f"[owner-bench] {label} iter {i + 1}/{iterations} done elapsed_ms={elapsed_ms:.2f} peak_bytes={int(peak)}",
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
                "median": round(statistics.median(times_ms), 3),
            },
            "peak_mem_bytes": {
                "min": min(peaks),
                "max": max(peaks),
                "mean": int(round(statistics.fmean(peaks))),
                "median": int(round(statistics.median(peaks))),
            },
        },
        "stable_output_signature": stable,
        "output_signature": signatures[0] if signatures else {},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark owner projection memory vs disk-spill modes.")
    parser.add_argument(
        "--case",
        default=None,
        help="Case NAME:OLD_PATH:NEW_PATH. Defaults to tests/fixtures/house_v1.ifc vs house_v2.ifc.",
    )
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=2)
    parser.add_argument(
        "--disk-threshold",
        type=int,
        default=1,
        help="Owner-pair threshold for disk mode (default: 1, effectively force spill).",
    )
    parser.add_argument("--out", default=None, help="Optional output JSON path.")
    args = parser.parse_args()

    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0")
    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1")

    repo_root = Path(__file__).resolve().parents[2]
    case = _parse_case(args.case) if args.case else _default_case(repo_root)
    print(f"[owner-bench] parsing old graph: {case.old_path}", file=sys.stderr, flush=True)
    parse_started = time.perf_counter()
    old_graph = parse_graph(str(case.old_path), profile="semantic_stable")
    print(
        f"[owner-bench] parsed old graph in {(time.perf_counter() - parse_started) * 1000.0:.1f} ms",
        file=sys.stderr,
        flush=True,
    )
    print(f"[owner-bench] parsing new graph: {case.new_path}", file=sys.stderr, flush=True)
    parse_started = time.perf_counter()
    new_graph = parse_graph(str(case.new_path), profile="semantic_stable")
    print(
        f"[owner-bench] parsed new graph in {(time.perf_counter() - parse_started) * 1000.0:.1f} ms",
        file=sys.stderr,
        flush=True,
    )

    print("[owner-bench] mode=memory", file=sys.stderr, flush=True)
    memory_stats = _benchmark(
        lambda: _with_owner_threshold(
            0,
            lambda: _result_signature(diff_graphs(old_graph, new_graph, profile="semantic_stable")),
        ),
        warmup=args.warmup,
        iterations=args.iterations,
        label="mode=memory",
    )
    print(f"[owner-bench] mode=disk_spill threshold={max(0, args.disk_threshold)}", file=sys.stderr, flush=True)
    disk_stats = _benchmark(
        lambda: _with_owner_threshold(
            args.disk_threshold,
            lambda: _result_signature(diff_graphs(old_graph, new_graph, profile="semantic_stable")),
        ),
        warmup=args.warmup,
        iterations=args.iterations,
        label="mode=disk_spill",
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "case": {
                "name": case.name,
                "old_path": str(case.old_path),
                "new_path": str(case.new_path),
            },
            "warmup": args.warmup,
            "iterations": args.iterations,
            "disk_threshold": max(0, args.disk_threshold),
        },
        "modes": {
            "memory": memory_stats,
            "disk_spill": disk_stats,
        },
        "equivalent_output": (
            memory_stats.get("output_signature", {}) == disk_stats.get("output_signature", {})
        ),
    }

    payload = canonical_json(report) + "\n"
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote owner projection benchmark to {out_path}")
    else:
        print(payload, end="")


if __name__ == "__main__":
    main()
