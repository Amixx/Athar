"""Benchmark diff engine runtime and peak Python memory usage.

Default cases benchmark same-file comparisons for:
- data/BasicHouse.ifc
- data/AdvancedProject.ifc
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import statistics
import sys
import time
import tracemalloc
from typing import Any, Callable

from athar.determinism import canonical_json, environment_fingerprint
from athar.diff_engine import diff_graphs, stream_diff_graphs
from athar.graph_parser import parse_graph
from athar.guid_policy import GUID_POLICY_CHOICES, GUID_POLICY_FAIL_FAST
from athar.profile_policy import DEFAULT_PROFILE, SUPPORTED_PROFILES


@dataclass(frozen=True)
class Case:
    name: str
    old_path: Path
    new_path: Path


def _default_cases(repo_root: Path) -> list[Case]:
    return [
        Case(
            name="basichouse_same_file",
            old_path=repo_root / "data" / "BasicHouse.ifc",
            new_path=repo_root / "data" / "BasicHouse.ifc",
        ),
        Case(
            name="advancedproject_same_file",
            old_path=repo_root / "data" / "AdvancedProject.ifc",
            new_path=repo_root / "data" / "AdvancedProject.ifc",
        ),
    ]


def _parse_case_arg(raw: str) -> Case:
    # NAME:OLD_PATH:NEW_PATH
    parts = raw.split(":", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid --case {raw!r}; expected NAME:OLD_PATH:NEW_PATH")
    name, old_path, new_path = parts
    return Case(name=name, old_path=Path(old_path), new_path=Path(new_path))


def _benchmark_repeated(
    fn: Callable[[], dict[str, Any]],
    *,
    warmup: int,
    iterations: int,
    label: str,
) -> dict[str, Any]:
    for i in range(warmup):
        print(f"[bench] {label} warmup {i + 1}/{warmup}", file=sys.stderr, flush=True)
        fn()

    time_samples_ms: list[float] = []
    peak_mem_samples_bytes: list[int] = []
    signatures: list[dict[str, Any]] = []

    for i in range(iterations):
        print(f"[bench] {label} iter {i + 1}/{iterations} start", file=sys.stderr, flush=True)
        tracemalloc.start()
        t0 = time.perf_counter()
        signature = fn()
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(
            f"[bench] {label} iter {i + 1}/{iterations} done elapsed_ms={elapsed_ms:.2f} peak_bytes={int(peak)}",
            file=sys.stderr,
            flush=True,
        )

        time_samples_ms.append(elapsed_ms)
        peak_mem_samples_bytes.append(int(peak))
        signatures.append(signature)

    stable = all(sig == signatures[0] for sig in signatures[1:]) if signatures else True
    return {
        "samples": {
            "time_ms": [round(v, 3) for v in time_samples_ms],
            "peak_mem_bytes": peak_mem_samples_bytes,
        },
        "summary": {
            "time_ms": _summarize_float_samples(time_samples_ms),
            "peak_mem_bytes": _summarize_int_samples(peak_mem_samples_bytes),
        },
        "stable_output_signature": stable,
        "output_signature": signatures[0] if signatures else {},
    }


def _summarize_named_float_samples(samples: list[dict[str, float]]) -> dict[str, Any]:
    by_name: dict[str, list[float]] = {}
    for sample in samples:
        for key, value in sample.items():
            by_name.setdefault(key, []).append(float(value))
    return {
        "samples": {key: [round(v, 3) for v in values] for key, values in sorted(by_name.items())},
        "summary": {key: _summarize_float_samples(values) for key, values in sorted(by_name.items())},
    }


def _summarize_float_samples(values: list[float]) -> dict[str, float]:
    if not values:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p95": 0.0}
    ordered = sorted(values)
    return {
        "min": round(ordered[0], 3),
        "max": round(ordered[-1], 3),
        "mean": round(statistics.fmean(values), 3),
        "median": round(statistics.median(values), 3),
        "p95": round(_percentile(ordered, 0.95), 3),
    }


def _summarize_int_samples(values: list[int]) -> dict[str, int]:
    if not values:
        return {"min": 0, "max": 0, "mean": 0, "median": 0, "p95": 0}
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "max": ordered[-1],
        "mean": int(round(statistics.fmean(values))),
        "median": int(round(statistics.median(values))),
        "p95": int(round(_percentile([float(v) for v in ordered], 0.95))),
    }


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    idx = int(round((len(sorted_values) - 1) * q))
    return sorted_values[idx]


def _run_case(
    case: Case,
    *,
    case_index: int,
    total_cases: int,
    profile: str,
    guid_policy: str,
    warmup: int,
    iterations: int,
    chunk_size: int,
    engine_timings: bool,
) -> dict[str, Any]:
    print(
        f"[bench] case {case_index}/{total_cases} name={case.name} parsing old graph {case.old_path}",
        file=sys.stderr,
        flush=True,
    )
    parse_started = time.perf_counter()
    old_graph = parse_graph(str(case.old_path), profile=profile)
    old_parse_ms = (time.perf_counter() - parse_started) * 1000.0
    print(
        f"[bench] case={case.name} parsed old graph in {old_parse_ms:.1f} ms",
        file=sys.stderr,
        flush=True,
    )
    print(
        f"[bench] case={case.name} parsing new graph {case.new_path}",
        file=sys.stderr,
        flush=True,
    )
    parse_started = time.perf_counter()
    new_graph = parse_graph(str(case.new_path), profile=profile)
    new_parse_ms = (time.perf_counter() - parse_started) * 1000.0
    print(
        f"[bench] case={case.name} parsed new graph in {new_parse_ms:.1f} ms",
        file=sys.stderr,
        flush=True,
    )

    print(f"[bench] case={case.name} metric=diff_graphs", file=sys.stderr, flush=True)
    diff_timing_samples: list[dict[str, float]] = []

    def _diff_call() -> dict[str, Any]:
        result = diff_graphs(
            old_graph,
            new_graph,
            profile=profile,
            guid_policy=guid_policy,
            timings=engine_timings,
        )
        if engine_timings:
            timings_ms = result.get("stats", {}).get("timings_ms")
            if isinstance(timings_ms, dict):
                normalized: dict[str, float] = {}
                for key, value in timings_ms.items():
                    if isinstance(value, (int, float)):
                        normalized[key] = float(value)
                if normalized:
                    diff_timing_samples.append(normalized)
        return {
            "base_change_count": len(result.get("base_changes", [])),
            "derived_marker_count": len(result.get("derived_markers", [])),
        }

    diff_stats = _benchmark_repeated(
        _diff_call,
        warmup=warmup,
        iterations=iterations,
        label=f"case={case.name} metric=diff_graphs",
    )
    if engine_timings and diff_timing_samples:
        diff_stats["engine_timings_ms"] = _summarize_named_float_samples(diff_timing_samples)
    print(
        f"[bench] case={case.name} metric=diff_graphs mean_ms={diff_stats['summary']['time_ms']['mean']}",
        file=sys.stderr,
        flush=True,
    )
    print(f"[bench] case={case.name} metric=stream_ndjson", file=sys.stderr, flush=True)
    ndjson_stats = _benchmark_repeated(
        lambda: _stream_signature(
            old_graph,
            new_graph,
            profile=profile,
            guid_policy=guid_policy,
            mode="ndjson",
            chunk_size=chunk_size,
        ),
        warmup=warmup,
        iterations=iterations,
        label=f"case={case.name} metric=stream_ndjson",
    )
    print(
        f"[bench] case={case.name} metric=stream_ndjson mean_ms={ndjson_stats['summary']['time_ms']['mean']}",
        file=sys.stderr,
        flush=True,
    )
    print(f"[bench] case={case.name} metric=stream_chunked_json", file=sys.stderr, flush=True)
    chunked_stats = _benchmark_repeated(
        lambda: _stream_signature(
            old_graph,
            new_graph,
            profile=profile,
            guid_policy=guid_policy,
            mode="chunked_json",
            chunk_size=chunk_size,
        ),
        warmup=warmup,
        iterations=iterations,
        label=f"case={case.name} metric=stream_chunked_json",
    )
    print(
        f"[bench] case={case.name} metric=stream_chunked_json mean_ms={chunked_stats['summary']['time_ms']['mean']}",
        file=sys.stderr,
        flush=True,
    )

    return {
        "case": {
            "name": case.name,
            "old_path": str(case.old_path),
            "new_path": str(case.new_path),
        },
        "parse_ms": {
            "old_graph": round(old_parse_ms, 3),
            "new_graph": round(new_parse_ms, 3),
            "total": round(old_parse_ms + new_parse_ms, 3),
        },
        "metrics": {
            "diff_graphs": diff_stats,
            "stream_diff_graphs_ndjson": ndjson_stats,
            "stream_diff_graphs_chunked_json": chunked_stats,
        },
    }


def _stream_signature(
    old_graph: dict,
    new_graph: dict,
    *,
    profile: str,
    guid_policy: str,
    mode: str,
    chunk_size: int,
) -> dict[str, Any]:
    line_count = 0
    byte_count = 0
    for line in stream_diff_graphs(
        old_graph,
        new_graph,
        profile=profile,
        guid_policy=guid_policy,
        mode=mode,
        chunk_size=chunk_size,
    ):
        line_count += 1
        byte_count += len(line.encode("utf-8"))
    return {"line_count": line_count, "bytes": byte_count}


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark low-level diff engine.")
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Benchmark case as NAME:OLD_PATH:NEW_PATH (repeatable).",
    )
    parser.add_argument("--warmup", type=int, default=1, help="Warmup runs per metric.")
    parser.add_argument("--iterations", type=int, default=2, help="Measured runs per metric.")
    parser.add_argument("--chunk-size", type=int, default=1000, help="Chunk size for chunked_json stream mode.")
    parser.add_argument(
        "--profile",
        choices=SUPPORTED_PROFILES,
        default=DEFAULT_PROFILE,
        help="Diff profile.",
    )
    parser.add_argument(
        "--guid-policy",
        choices=GUID_POLICY_CHOICES,
        default=GUID_POLICY_FAIL_FAST,
        help="Guid policy.",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Optional output path for JSON report.",
    )
    parser.add_argument(
        "--engine-timings",
        action="store_true",
        help="Collect per-stage diff engine timings (`stats.timings_ms`) in diff_graphs benchmark output.",
    )
    args = parser.parse_args()

    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0")
    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1")
    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be >= 1")

    repo_root = Path(__file__).resolve().parents[2]
    cases = [_parse_case_arg(raw) for raw in args.case] if args.case else _default_cases(repo_root)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": environment_fingerprint(),
        "config": {
            "warmup": args.warmup,
            "iterations": args.iterations,
            "chunk_size": args.chunk_size,
            "profile": args.profile,
            "guid_policy": args.guid_policy,
            "engine_timings": args.engine_timings,
        },
        "results": [],
    }
    total_cases = len(cases)
    for idx, case in enumerate(cases, start=1):
        report["results"].append(_run_case(
            case,
            case_index=idx,
            total_cases=total_cases,
            profile=args.profile,
            guid_policy=args.guid_policy,
            warmup=args.warmup,
            iterations=args.iterations,
            chunk_size=args.chunk_size,
            engine_timings=args.engine_timings,
        ))

    payload = canonical_json(report) + "\n"
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote benchmark report to {out_path}")
    else:
        print(payload, end="")


if __name__ == "__main__":
    main()
