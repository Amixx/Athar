"""Benchmark diff engine runtime and peak Python memory usage.

Default cases benchmark same-file comparisons for:
- data/BasicHouse.ifc
- data/AdvancedProject.ifc
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import sys
import threading
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
    heartbeat_s: int,
    expected_ms: float | None = None,
    progress_probe: Callable[[], dict[str, Any] | None] | None = None,
    progress_update: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    for i in range(warmup):
        print(f"[bench] {label} warmup {i + 1}/{warmup}", file=sys.stderr, flush=True)
        fn()

    time_samples_ms: list[float] = []
    peak_mem_samples_bytes: list[int] = []
    signatures: list[dict[str, Any]] = []

    for i in range(iterations):
        print(f"[bench] {label} iter {i + 1}/{iterations} start", file=sys.stderr, flush=True)
        _emit_progress_update(progress_update, {
            "status": "running",
            "iteration": i + 1,
            "iterations": iterations,
            "event": "iter_start",
        })
        tracemalloc.start()
        t0 = time.perf_counter()
        stop = threading.Event()

        def _heartbeat() -> None:
            while not stop.wait(timeout=float(heartbeat_s)):
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                extra = ""
                probe = _probe_progress(progress_probe)
                if probe:
                    stage = probe.get("stage")
                    status = probe.get("status")
                    counts = _counts_from_probe(probe)
                    if isinstance(stage, str):
                        extra += f" stage={stage}"
                    if isinstance(status, str):
                        extra += f" status={status}"
                    if counts is not None:
                        completed, total = counts
                        extra += f" items={completed}/{total}"
                    probe_eta = _progress_eta_from_probe(elapsed_ms, probe)
                    if probe_eta is not None:
                        observed_clamped, eta_seconds = probe_eta
                        pct = observed_clamped * 100.0
                        eta_text = _format_eta(eta_seconds)
                        extra += f" progress~{pct:.1f}% eta~{eta_text}"
                        _emit_progress_update(progress_update, {
                            "status": "running",
                            "event": "heartbeat",
                            "iteration": i + 1,
                            "iterations": iterations,
                            "elapsed_ms": round(elapsed_ms, 1),
                            "progress_fraction": round(observed_clamped, 6),
                            "eta_seconds": round(max(eta_seconds, 0.0), 3),
                            "eta_text": eta_text,
                            "probe": probe,
                        })
                    else:
                        progress_eta = _progress_eta(elapsed_ms, expected_ms)
                        if progress_eta is not None:
                            progress_pct, eta_text = progress_eta
                            extra += f" progress~{progress_pct} eta~{eta_text}"
                            _emit_progress_update(progress_update, {
                                "status": "running",
                                "event": "heartbeat",
                                "iteration": i + 1,
                                "iterations": iterations,
                                "elapsed_ms": round(elapsed_ms, 1),
                                "progress_fraction": round(float(progress_pct.rstrip("%")) / 100.0, 6),
                                "eta_text": eta_text,
                            })
                else:
                    progress_eta = _progress_eta(elapsed_ms, expected_ms)
                    if progress_eta is not None:
                        progress_pct, eta_text = progress_eta
                        extra = f" progress~{progress_pct} eta~{eta_text}"
                        _emit_progress_update(progress_update, {
                            "status": "running",
                            "event": "heartbeat",
                            "iteration": i + 1,
                            "iterations": iterations,
                            "elapsed_ms": round(elapsed_ms, 1),
                            "progress_fraction": round(float(progress_pct.rstrip("%")) / 100.0, 6),
                            "eta_text": eta_text,
                        })
                print(
                    f"[bench] {label} iter {i + 1}/{iterations} heartbeat elapsed_ms={elapsed_ms:.1f}{extra}",
                    file=sys.stderr,
                    flush=True,
                )

        heartbeat_thread: threading.Thread | None = None
        if heartbeat_s > 0:
            heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
            heartbeat_thread.start()

        signature = fn()
        stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=1.0)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(
            f"[bench] {label} iter {i + 1}/{iterations} done elapsed_ms={elapsed_ms:.2f} peak_bytes={int(peak)}",
            file=sys.stderr,
            flush=True,
        )
        _emit_progress_update(progress_update, {
            "status": "running",
            "event": "iter_done",
            "iteration": i + 1,
            "iterations": iterations,
            "elapsed_ms": round(elapsed_ms, 3),
            "peak_mem_bytes": int(peak),
        })

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


def _emit_progress_update(
    callback: Callable[[dict[str, Any]], None] | None,
    payload: dict[str, Any],
) -> None:
    if callback is None:
        return
    try:
        callback(payload)
    except Exception:
        return


def _summarize_named_float_samples(samples: list[dict[str, float]]) -> dict[str, Any]:
    by_name: dict[str, list[float]] = {}
    for sample in samples:
        for key, value in sample.items():
            by_name.setdefault(key, []).append(float(value))
    return {
        "samples": {key: [round(v, 3) for v in values] for key, values in sorted(by_name.items())},
        "summary": {key: _summarize_float_samples(values) for key, values in sorted(by_name.items())},
    }


def _progress_eta(elapsed_ms: float, expected_ms: float | None) -> tuple[str, str] | None:
    if expected_ms is None or expected_ms <= 0:
        return None
    progress_raw = max(elapsed_ms / expected_ms, 0.0)
    progress = min(progress_raw, 0.99)
    if elapsed_ms > expected_ms:
        overrun_s = (elapsed_ms - expected_ms) / 1000.0
        return (f"{progress * 100.0:.1f}%", f"overrun {_format_eta(overrun_s)}")
    eta_s = (expected_ms - elapsed_ms) / 1000.0
    return (f"{progress * 100.0:.1f}%", _format_eta(eta_s))


def _format_eta(seconds: float) -> str:
    total = max(int(round(seconds)), 0)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _probe_progress(progress_probe: Callable[[], dict[str, Any] | None] | None) -> dict[str, Any] | None:
    if progress_probe is None:
        return None
    try:
        snapshot = progress_probe()
    except Exception:
        return None
    if not isinstance(snapshot, dict):
        return None
    return dict(snapshot)


def _counts_from_probe(probe: dict[str, Any]) -> tuple[int, int] | None:
    completed = probe.get("completed")
    total = probe.get("total")
    if not (isinstance(completed, int) and isinstance(total, int) and total > 0):
        completed = probe.get("completed_steps")
        total = probe.get("total_steps")
    if not (isinstance(completed, int) and isinstance(total, int) and total > 0):
        return None
    return (max(completed, 0), total)


def _progress_eta_from_probe(elapsed_ms: float, probe: dict[str, Any]) -> tuple[float, float] | None:
    counts = _counts_from_probe(probe)
    if counts is not None:
        completed, total = counts
        if completed <= 0:
            return None
        progress = min(max(completed / total, 0.0), 1.0)
        if progress >= 1.0:
            return (1.0, 0.0)
        elapsed_s = max(elapsed_ms / 1000.0, 0.0)
        eta_s = elapsed_s * (total - completed) / completed
        return (progress, max(eta_s, 0.0))

    observed = probe.get("overall_progress")
    if isinstance(observed, (int, float)):
        progress = min(max(float(observed), 0.0), 1.0)
        if progress <= 0.0:
            return None
        if progress >= 1.0:
            return (1.0, 0.0)
        elapsed_s = max(elapsed_ms / 1000.0, 0.0)
        eta_s = elapsed_s * (1.0 - progress) / progress
        return (progress, max(eta_s, 0.0))
    return None


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
    heartbeat_s: int,
    progress_update: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    _emit_progress_update(progress_update, {
        "status": "running",
        "phase": "parse_old",
    })
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
    _emit_progress_update(progress_update, {
        "status": "running",
        "phase": "parse_new",
        "parse_ms": {"old_graph": round(old_parse_ms, 3)},
    })
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
    _emit_progress_update(progress_update, {
        "status": "running",
        "phase": "metrics",
        "parse_ms": {
            "old_graph": round(old_parse_ms, 3),
            "new_graph": round(new_parse_ms, 3),
            "total": round(old_parse_ms + new_parse_ms, 3),
        },
    })

    print(f"[bench] case={case.name} metric=diff_graphs", file=sys.stderr, flush=True)
    diff_timing_samples: list[dict[str, float]] = []
    diff_progress_state: dict[str, Any] = {
        "stage": "prepare_context",
        "status": "start",
        "overall_progress": 0.0,
    }
    parse_total_ms = old_parse_ms + new_parse_ms
    # Coarse prior: full diff usually dominates parse by a large constant factor.
    diff_expected_ms = max(parse_total_ms * 8.0, 120000.0)
    print(
        f"[bench] case={case.name} metric=diff_graphs eta_model=heuristic expected_ms={diff_expected_ms:.1f}",
        file=sys.stderr,
        flush=True,
    )

    def _diff_call() -> dict[str, Any]:
        diff_progress_state.clear()
        diff_progress_state.update({
            "stage": "prepare_context",
            "status": "start",
            "overall_progress": 0.0,
        })

        def _on_progress(event: dict[str, Any]) -> None:
            diff_progress_state.update(event)

        result = diff_graphs(
            old_graph,
            new_graph,
            profile=profile,
            guid_policy=guid_policy,
            timings=engine_timings,
            progress_callback=_on_progress,
        )
        diff_progress_state.update({
            "stage": "done",
            "status": "done",
            "overall_progress": 1.0,
        })
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
        heartbeat_s=heartbeat_s,
        expected_ms=diff_expected_ms,
        progress_probe=lambda: diff_progress_state,
        progress_update=(
            lambda payload: _emit_progress_update(progress_update, {
                "status": "running",
                "phase": "metrics",
                "metric": "diff_graphs",
                **payload,
            })
        ),
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
        heartbeat_s=heartbeat_s,
        expected_ms=max(float(diff_stats["summary"]["time_ms"]["mean"]) * 1.2, 1000.0),
        progress_update=(
            lambda payload: _emit_progress_update(progress_update, {
                "status": "running",
                "phase": "metrics",
                "metric": "stream_ndjson",
                **payload,
            })
        ),
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
        heartbeat_s=heartbeat_s,
        expected_ms=max(float(ndjson_stats["summary"]["time_ms"]["mean"]) * 1.1, 1000.0),
        progress_update=(
            lambda payload: _emit_progress_update(progress_update, {
                "status": "running",
                "phase": "metrics",
                "metric": "stream_chunked_json",
                **payload,
            })
        ),
    )
    print(
        f"[bench] case={case.name} metric=stream_chunked_json mean_ms={chunked_stats['summary']['time_ms']['mean']}",
        file=sys.stderr,
        flush=True,
    )

    out = {
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
    _emit_progress_update(progress_update, {
        "status": "running",
        "phase": "done",
        "metrics_summary": {
            "diff_graphs_mean_ms": diff_stats["summary"]["time_ms"]["mean"],
            "stream_ndjson_mean_ms": ndjson_stats["summary"]["time_ms"]["mean"],
            "stream_chunked_json_mean_ms": chunked_stats["summary"]["time_ms"]["mean"],
        },
    })
    return out


def _write_progress(path: Path | None, lock: threading.Lock, state: dict[str, Any]) -> None:
    if path is None:
        return
    with lock:
        payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(path.name + ".tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        tmp_path.replace(path)


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
    parser.add_argument(
        "--heartbeat-s",
        type=int,
        default=30,
        help="Print per-iteration heartbeat logs every N seconds while metric execution is in progress (0 disables).",
    )
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
    parser.add_argument(
        "--progress-file",
        default=None,
        help="Optional path to a live progress JSON sidecar updated throughout execution.",
    )
    args = parser.parse_args()

    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0")
    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1")
    if args.heartbeat_s < 0:
        raise ValueError("--heartbeat-s must be >= 0")
    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be >= 1")

    repo_root = Path(__file__).resolve().parents[2]
    cases = [_parse_case_arg(raw) for raw in args.case] if args.case else _default_cases(repo_root)
    progress_path = Path(args.progress_file) if args.progress_file else None
    progress_lock = threading.Lock()
    started_at = datetime.now(timezone.utc).isoformat()
    progress_state: dict[str, Any] = {
        "state": "running",
        "started_at": started_at,
        "updated_at": started_at,
        "total_cases": len(cases),
        "completed_cases": 0,
        "current_case": None,
        "config": {
            "warmup": args.warmup,
            "iterations": args.iterations,
            "heartbeat_s": args.heartbeat_s,
            "chunk_size": args.chunk_size,
            "profile": args.profile,
            "guid_policy": args.guid_policy,
            "engine_timings": args.engine_timings,
        },
    }

    def _progress_update(delta: dict[str, Any]) -> None:
        progress_state.update(delta)
        progress_state["updated_at"] = datetime.now(timezone.utc).isoformat()
        _write_progress(progress_path, progress_lock, progress_state)

    _write_progress(progress_path, progress_lock, progress_state)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": environment_fingerprint(),
        "config": {
            "warmup": args.warmup,
            "iterations": args.iterations,
            "heartbeat_s": args.heartbeat_s,
            "chunk_size": args.chunk_size,
            "profile": args.profile,
            "guid_policy": args.guid_policy,
            "engine_timings": args.engine_timings,
        },
        "results": [],
    }
    total_cases = len(cases)
    try:
        for idx, case in enumerate(cases, start=1):
            _progress_update({
                "state": "running",
                "completed_cases": idx - 1,
                "current_case": {
                    "index": idx,
                    "total": total_cases,
                    "name": case.name,
                    "phase": "start",
                },
            })
            case_start = time.perf_counter()

            def _case_progress(delta: dict[str, Any]) -> None:
                _progress_update({
                    "state": "running",
                    "completed_cases": idx - 1,
                    "current_case": {
                        "index": idx,
                        "total": total_cases,
                        "name": case.name,
                        **delta,
                    },
                })

            case_result = _run_case(
                case,
                case_index=idx,
                total_cases=total_cases,
                profile=args.profile,
                guid_policy=args.guid_policy,
                warmup=args.warmup,
                iterations=args.iterations,
                chunk_size=args.chunk_size,
                engine_timings=args.engine_timings,
                heartbeat_s=args.heartbeat_s,
                progress_update=_case_progress,
            )
            report["results"].append(case_result)
            _progress_update({
                "state": "running",
                "completed_cases": idx,
                "current_case": {
                    "index": idx,
                    "total": total_cases,
                    "name": case.name,
                    "phase": "completed",
                    "elapsed_ms": round((time.perf_counter() - case_start) * 1000.0, 3),
                },
            })
    except Exception as exc:
        _progress_update({
            "state": "failed",
            "error": str(exc),
        })
        raise

    payload = canonical_json(report) + "\n"
    if args.out:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(payload, encoding="utf-8")
        print(f"Wrote benchmark report to {out_path}")
    else:
        print(payload, end="")
        out_path = None

    _progress_update({
        "state": "completed",
        "completed_cases": total_cases,
        "current_case": None,
        "report_path": str(out_path) if args.out else None,
    })


if __name__ == "__main__":
    main()
