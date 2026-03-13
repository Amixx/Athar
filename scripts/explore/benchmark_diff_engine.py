"""Benchmark diff engine runtime and peak Python memory usage.

Default cases:
- house_v1_v2: tests/fixtures/house_v1.ifc vs house_v2.ifc (large, low-GUID cross-version diff)
- basichouse_v1_v2: tests/fixtures/BasicHouse.ifc vs tests/fixtures/BasicHouse_modified.ifc (medium, high-GUID cross-version diff)
- basichouse_same_file: data/BasicHouse.ifc vs itself (short-circuit)
- advancedproject_same_file: data/AdvancedProject.ifc vs itself (short-circuit)
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import statistics
import sys
import threading
import time
import tracemalloc
from typing import Any, Callable

from athar.graph.determinism import canonical_json, environment_fingerprint
from athar.diff.engine import diff_graphs, stream_diff_graphs
from athar.graph.graph_parser import parse_graph
from athar.diff.guid_policy import GUID_POLICY_CHOICES, GUID_POLICY_FAIL_FAST
from athar.graph.profile_policy import DEFAULT_PROFILE, SUPPORTED_PROFILES

_METRIC_DIFF = "diff_graphs"
_METRIC_STREAM_NDJSON = "stream_ndjson"
_METRIC_STREAM_CHUNKED = "stream_chunked_json"
_METRIC_CHOICES = (_METRIC_DIFF, _METRIC_STREAM_NDJSON, _METRIC_STREAM_CHUNKED)


@dataclass(frozen=True)
class Case:
    name: str
    old_path: Path
    new_path: Path


def _default_cases(repo_root: Path) -> list[Case]:
    return [
        Case(
            name="house_v1_v2",
            old_path=repo_root / "tests" / "fixtures" / "house_v1.ifc",
            new_path=repo_root / "tests" / "fixtures" / "house_v2.ifc",
        ),
        Case(
            name="basichouse_v1_v2",
            old_path=repo_root / "tests" / "fixtures" / "BasicHouse.ifc",
            new_path=repo_root / "tests" / "fixtures" / "BasicHouse_modified.ifc",
        ),
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


def _default_report_path(repo_root: Path) -> Path:
    perf_dir = repo_root / "docs" / "perf"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    label = os.getenv("ATHAR_BENCHMARK_NAME", "benchmark_diff_engine").strip() or "benchmark_diff_engine"
    return perf_dir / f"{label}_{timestamp}.json"


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
                    f"[bench] {label} iter {i + 1}/{iterations} heartbeat elapsed={_format_duration_ms(elapsed_ms)}{extra}",
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
            f"[bench] {label} iter {i + 1}/{iterations} done elapsed={_format_duration_ms(elapsed_ms)} peak_bytes={int(peak)}",
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


def _format_duration_ms(milliseconds: float) -> str:
    total_ms = max(float(milliseconds), 0.0)
    minutes, rem_ms = divmod(total_ms, 60_000.0)
    seconds, millis = divmod(rem_ms, 1_000.0)
    return f"{int(minutes)}m {int(seconds)}s {millis:.1f}ms"


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

    counts = _counts_from_probe(probe)
    if counts is not None:
        completed, total = counts
        if completed <= 1:
            return None
        progress = min(max(completed / total, 0.0), 1.0)
        if progress >= 1.0:
            return (1.0, 0.0)
        elapsed_s = max(elapsed_ms / 1000.0, 0.0)
        eta_s = elapsed_s * (total - completed) / completed
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
    metrics: set[str],
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
        f"[bench] case={case.name} parsed old graph in {_format_duration_ms(old_parse_ms)}",
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
    if case.old_path == case.new_path:
        new_graph = old_graph
        new_parse_ms = 0.0
        print(
            f"[bench] case={case.name} reused old graph for new graph (same path)",
            file=sys.stderr,
            flush=True,
        )
    else:
        parse_started = time.perf_counter()
        new_graph = parse_graph(str(case.new_path), profile=profile)
        new_parse_ms = (time.perf_counter() - parse_started) * 1000.0
        print(
            f"[bench] case={case.name} parsed new graph in {_format_duration_ms(new_parse_ms)}",
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

    diff_stats: dict[str, Any] | None = None
    base_count = 0
    marker_count = 0
    parse_total_ms = old_parse_ms + new_parse_ms
    diff_timing_samples: list[dict[str, float]] = []
    diff_progress_state: dict[str, Any] = {"stage": "prepare_context", "status": "start", "overall_progress": 0.0}

    def _diff_call() -> dict[str, Any]:
        diff_progress_state.clear()
        diff_progress_state.update({
            "stage": "prepare_context",
            "status": "start",
            "overall_progress": 0.0,
        })

        def _on_progress(event: dict[str, Any]) -> None:
            stage = event.get("stage")
            status = event.get("status")
            if stage != diff_progress_state.get("stage") or status == "start":
                for key in (
                    "completed",
                    "total",
                    "completed_steps",
                    "total_steps",
                    "emitted_changes",
                    "step",
                    "stage_progress",
                ):
                    diff_progress_state.pop(key, None)
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

    if _METRIC_DIFF in metrics:
        print(f"[bench] case={case.name} metric=diff_graphs", file=sys.stderr, flush=True)
        # Coarse prior: full diff usually dominates parse by a large constant factor.
        diff_expected_ms = max(parse_total_ms * 8.0, 120000.0)
        print(
            f"[bench] case={case.name} metric=diff_graphs eta_model=heuristic expected={_format_duration_ms(diff_expected_ms)}",
            file=sys.stderr,
            flush=True,
        )
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
                    "metric": _METRIC_DIFF,
                    **payload,
                })
            ),
        )
        if engine_timings and diff_timing_samples:
            diff_stats["engine_timings_ms"] = _summarize_named_float_samples(diff_timing_samples)
        print(
            f"[bench] case={case.name} metric=diff_graphs mean={_format_duration_ms(diff_stats['summary']['time_ms']['mean'])}",
            file=sys.stderr,
            flush=True,
        )
        diff_signature = diff_stats.get("output_signature", {})
        base_count = int(diff_signature.get("base_change_count", 0))
        marker_count = int(diff_signature.get("derived_marker_count", 0))

    ndjson_stats: dict[str, Any] | None = None
    ndjson_progress_state: dict[str, Any] = {
        "stage": "stream_emit",
        "status": "start",
    }
    expected_ndjson_records = _expected_stream_record_count(
        mode="ndjson",
        base_change_count=base_count,
        derived_marker_count=marker_count,
        chunk_size=chunk_size,
    )

    def _stream_ndjson_call() -> dict[str, Any]:
        return _stream_signature(
            old_graph,
            new_graph,
            profile=profile,
            guid_policy=guid_policy,
            mode="ndjson",
            chunk_size=chunk_size,
            progress_state=ndjson_progress_state,
            expected_records=expected_ndjson_records,
        )

    if _METRIC_STREAM_NDJSON in metrics:
        print(f"[bench] case={case.name} metric=stream_ndjson", file=sys.stderr, flush=True)
        ndjson_expected_ms = max(float(diff_stats["summary"]["time_ms"]["mean"]) * 1.2, 1000.0) if diff_stats is not None else max(parse_total_ms * 8.0, 120000.0)
        ndjson_stats = _benchmark_repeated(
            _stream_ndjson_call,
            warmup=warmup,
            iterations=iterations,
            label=f"case={case.name} metric=stream_ndjson",
            heartbeat_s=heartbeat_s,
            expected_ms=ndjson_expected_ms,
            progress_probe=lambda: ndjson_progress_state,
            progress_update=(
                lambda payload: _emit_progress_update(progress_update, {
                    "status": "running",
                    "phase": "metrics",
                    "metric": _METRIC_STREAM_NDJSON,
                    **payload,
                })
            ),
        )
        print(
            f"[bench] case={case.name} metric=stream_ndjson mean={_format_duration_ms(ndjson_stats['summary']['time_ms']['mean'])}",
            file=sys.stderr,
            flush=True,
        )

    chunked_stats: dict[str, Any] | None = None
    chunked_progress_state: dict[str, Any] = {
        "stage": "stream_emit",
        "status": "start",
    }
    expected_chunked_records = _expected_stream_record_count(
        mode="chunked_json",
        base_change_count=base_count,
        derived_marker_count=marker_count,
        chunk_size=chunk_size,
    )

    def _stream_chunked_call() -> dict[str, Any]:
        return _stream_signature(
            old_graph,
            new_graph,
            profile=profile,
            guid_policy=guid_policy,
            mode="chunked_json",
            chunk_size=chunk_size,
            progress_state=chunked_progress_state,
            expected_records=expected_chunked_records,
        )

    if _METRIC_STREAM_CHUNKED in metrics:
        print(f"[bench] case={case.name} metric=stream_chunked_json", file=sys.stderr, flush=True)
        if ndjson_stats is not None:
            chunked_expected_ms = max(float(ndjson_stats["summary"]["time_ms"]["mean"]) * 1.1, 1000.0)
        elif diff_stats is not None:
            chunked_expected_ms = max(float(diff_stats["summary"]["time_ms"]["mean"]) * 1.2, 1000.0)
        else:
            chunked_expected_ms = max(parse_total_ms * 8.0, 120000.0)
        chunked_stats = _benchmark_repeated(
            _stream_chunked_call,
            warmup=warmup,
            iterations=iterations,
            label=f"case={case.name} metric=stream_chunked_json",
            heartbeat_s=heartbeat_s,
            expected_ms=chunked_expected_ms,
            progress_probe=lambda: chunked_progress_state,
            progress_update=(
                lambda payload: _emit_progress_update(progress_update, {
                    "status": "running",
                    "phase": "metrics",
                    "metric": _METRIC_STREAM_CHUNKED,
                    **payload,
                })
            ),
        )
        print(
            f"[bench] case={case.name} metric=stream_chunked_json mean={_format_duration_ms(chunked_stats['summary']['time_ms']['mean'])}",
            file=sys.stderr,
            flush=True,
        )

    metric_out: dict[str, Any] = {}
    if diff_stats is not None:
        metric_out["diff_graphs"] = diff_stats
    if ndjson_stats is not None:
        metric_out["stream_diff_graphs_ndjson"] = ndjson_stats
    if chunked_stats is not None:
        metric_out["stream_diff_graphs_chunked_json"] = chunked_stats
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
        "metrics": metric_out,
    }
    summary_payload: dict[str, Any] = {}
    if diff_stats is not None:
        summary_payload["diff_graphs_mean_ms"] = diff_stats["summary"]["time_ms"]["mean"]
    if ndjson_stats is not None:
        summary_payload["stream_ndjson_mean_ms"] = ndjson_stats["summary"]["time_ms"]["mean"]
    if chunked_stats is not None:
        summary_payload["stream_chunked_json_mean_ms"] = chunked_stats["summary"]["time_ms"]["mean"]
    _emit_progress_update(progress_update, {"status": "running", "phase": "done", "metrics_summary": summary_payload})
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
    progress_state: dict[str, Any] | None = None,
    expected_records: int | None = None,
) -> dict[str, Any]:
    if progress_state is not None:
        progress_state.clear()
        progress_state.update({
            "stage": "stream_emit",
            "status": "start",
        })
    line_count = 0
    byte_count = 0
    heartbeat_interval = 500
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
        if progress_state is not None and (
            line_count == 1
            or line_count % heartbeat_interval == 0
            or (expected_records is not None and line_count >= expected_records)
        ):
            progress_state.update({
                "stage": "stream_emit",
                "status": "running",
                "completed": line_count,
                "total": expected_records,
                "bytes": byte_count,
            })
    if progress_state is not None:
        progress_state.update({
            "stage": "stream_emit",
            "status": "done",
            "completed": line_count,
            "total": expected_records if expected_records is not None else line_count,
            "bytes": byte_count,
        })
    return {"line_count": line_count, "bytes": byte_count}


def _expected_stream_record_count(
    *,
    mode: str,
    base_change_count: int,
    derived_marker_count: int,
    chunk_size: int,
) -> int | None:
    if base_change_count < 0 or derived_marker_count < 0:
        return None
    if mode == "ndjson":
        return 2 + base_change_count + derived_marker_count
    if mode == "chunked_json":
        return (
            2
            + int(math.ceil(base_change_count / chunk_size))
            + int(math.ceil(derived_marker_count / chunk_size))
        )
    return None


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
        "--metric",
        action="append",
        choices=_METRIC_CHOICES,
        default=[],
        help=(
            "Benchmark metric(s) to run (repeatable). "
            "Defaults to all: diff_graphs, stream_ndjson, stream_chunked_json."
        ),
    )
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
        help=(
            "Optional output path for JSON report. "
            "If omitted, writes to docs/perf/<label>_<timestamp>.json "
            "(label from ATHAR_BENCHMARK_NAME or benchmark_diff_engine)."
        ),
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
    selected_metrics = set(args.metric) if args.metric else set(_METRIC_CHOICES)
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
            "metrics": sorted(selected_metrics),
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
            "metrics": sorted(selected_metrics),
        },
        "results": [],
    }
    run_started = time.perf_counter()
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
                metrics=selected_metrics,
                progress_update=_case_progress,
            )
            report["results"].append(case_result)
            case_elapsed_ms = (time.perf_counter() - case_start) * 1000.0
            _progress_update({
                "state": "running",
                "completed_cases": idx,
                "current_case": {
                    "index": idx,
                    "total": total_cases,
                    "name": case.name,
                    "phase": "completed",
                    "elapsed_ms": round(case_elapsed_ms, 3),
                    "elapsed_text": _format_duration_ms(case_elapsed_ms),
                },
            })
    except Exception as exc:
        _progress_update({
            "state": "failed",
            "error": str(exc),
        })
        raise

    total_elapsed_ms = (time.perf_counter() - run_started) * 1000.0
    report["run_summary"] = {
        "total_elapsed_ms": round(total_elapsed_ms, 3),
        "total_elapsed_text": _format_duration_ms(total_elapsed_ms),
        "total_cases": total_cases,
    }
    payload = canonical_json(report) + "\n"
    out_path = Path(args.out) if args.out else _default_report_path(repo_root)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(payload, encoding="utf-8")
    print(f"Wrote benchmark report to {out_path}")

    _progress_update({
        "state": "completed",
        "completed_cases": total_cases,
        "current_case": None,
        "report_path": str(out_path),
        "total_elapsed_ms": round(total_elapsed_ms, 3),
        "total_elapsed_text": _format_duration_ms(total_elapsed_ms),
    })
    print(
        f"[bench] completed cases={total_cases}/{total_cases} total_elapsed={_format_duration_ms(total_elapsed_ms)}",
        file=sys.stderr,
        flush=True,
    )


if __name__ == "__main__":
    main()
