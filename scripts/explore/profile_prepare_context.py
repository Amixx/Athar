"""Profile only prepare_diff_context on a fixed graph pair.

Parses old/new IFC graphs once, then repeatedly runs prepare_diff_context while
capturing:
- wall time
- peak Python memory (tracemalloc)
- per-stage timing_collector breakdown
- optional cProfile top table

This isolates context/identity/matching prep without base-change emission.
"""

from __future__ import annotations

import argparse
import cProfile
from dataclasses import dataclass
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import pstats
import statistics
import sys
import threading
import time
import tracemalloc
from typing import Any, Callable

from athar.diff_engine_context import prepare_diff_context
from athar.geometry_policy import GEOMETRY_POLICY_CHOICES, GEOMETRY_POLICY_STRICT_SYNTAX
from athar.graph_parser import parse_graph
from athar.guid_policy import GUID_POLICY_CHOICES, GUID_POLICY_FAIL_FAST
from athar.profile_policy import DEFAULT_PROFILE, SUPPORTED_PROFILES


@dataclass(frozen=True)
class RunResult:
    elapsed_ms: float
    peak_mem_bytes: int
    timings_ms: dict[str, float]
    signature: dict[str, Any]


def _format_duration_ms(milliseconds: float) -> str:
    total_ms = max(float(milliseconds), 0.0)
    minutes, rem_ms = divmod(total_ms, 60_000.0)
    seconds, millis = divmod(rem_ms, 1_000.0)
    return f"{int(minutes)}m {int(seconds)}s {millis:.1f}ms"


def _format_eta(seconds: float) -> str:
    total = max(int(round(seconds)), 0)
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _summarize_float_samples(values: list[float]) -> dict[str, float]:
    ordered = sorted(float(v) for v in values)
    if not ordered:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "median": 0.0, "p95": 0.0}

    def _pct(p: float) -> float:
        if len(ordered) == 1:
            return ordered[0]
        idx = (len(ordered) - 1) * p
        lo = int(idx)
        hi = min(lo + 1, len(ordered) - 1)
        frac = idx - lo
        return ordered[lo] * (1 - frac) + ordered[hi] * frac

    return {
        "min": round(ordered[0], 3),
        "max": round(ordered[-1], 3),
        "mean": round(statistics.fmean(ordered), 3),
        "median": round(statistics.median(ordered), 3),
        "p95": round(_pct(0.95), 3),
    }


def _summarize_int_samples(values: list[int]) -> dict[str, int]:
    ordered = sorted(int(v) for v in values)
    if not ordered:
        return {"min": 0, "max": 0, "mean": 0, "median": 0, "p95": 0}

    def _pct(p: float) -> int:
        if len(ordered) == 1:
            return ordered[0]
        idx = (len(ordered) - 1) * p
        lo = int(idx)
        hi = min(lo + 1, len(ordered) - 1)
        frac = idx - lo
        return int(round(ordered[lo] * (1 - frac) + ordered[hi] * frac))

    return {
        "min": ordered[0],
        "max": ordered[-1],
        "mean": int(round(statistics.fmean(ordered))),
        "median": int(round(statistics.median(ordered))),
        "p95": _pct(0.95),
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


def _load_matcher_policy(raw_json: str | None) -> dict[str, dict[str, Any]] | None:
    if raw_json is None:
        return None
    payload = json.loads(raw_json)
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise ValueError("--matcher-policy-json must decode to an object")
    return payload


def _run_prepare_once(
    *,
    old_graph: dict[str, Any],
    new_graph: dict[str, Any],
    profile: str,
    geometry_policy: str,
    guid_policy: str,
    matcher_policy: dict[str, dict[str, Any]] | None,
    progress_state: dict[str, Any],
    cprof: cProfile.Profile | None,
) -> RunResult:
    timing_collector: dict[str, float] = {}

    progress_state.clear()
    progress_state.update({
        "stage": "prepare_context",
        "status": "start",
        "completed_steps": 0,
        "total_steps": 0,
        "stage_progress": 0.0,
    })

    def _on_progress(event: dict[str, Any]) -> None:
        progress_state.update(event)
        progress_state["stage"] = "prepare_context"

    tracemalloc.start()
    started = time.perf_counter()
    if cprof is not None:
        cprof.enable()
    context = prepare_diff_context(
        old_graph,
        new_graph,
        profile=profile,
        geometry_policy=geometry_policy,
        guid_policy=guid_policy,
        matcher_policy=matcher_policy,
        timing_collector=timing_collector,
        progress_callback=_on_progress,
    )
    if cprof is not None:
        cprof.disable()
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    progress_state.update({
        "status": "done",
        "stage": "prepare_context",
        "elapsed_ms": round(elapsed_ms, 1),
        "stage_progress": 1.0,
    })

    signature = {
        "old_by_id": len(context.get("old_by_id", {})),
        "new_by_id": len(context.get("new_by_id", {})),
        "matched": int(context.get("stats", {}).get("matched", 0)),
        "ambiguous": int(context.get("stats", {}).get("ambiguous", 0)),
    }
    return RunResult(
        elapsed_ms=elapsed_ms,
        peak_mem_bytes=int(peak),
        timings_ms=timing_collector,
        signature=signature,
    )


def _profile_top(profile: cProfile.Profile, *, sort_by: str, top_n: int) -> list[dict[str, Any]]:
    stream = io.StringIO()
    stats = pstats.Stats(profile, stream=stream)
    stats.strip_dirs()
    stats.sort_stats(sort_by)
    stats.print_stats(top_n)
    raw_text = stream.getvalue().strip()

    entries: list[dict[str, Any]] = []
    stats_obj = stats.stats
    ordered = sorted(
        stats_obj.items(),
        key=(lambda kv: kv[1][3] if sort_by == "cumulative" else kv[1][2]),
        reverse=True,
    )
    for func, metric in ordered[:top_n]:
        cc, nc, tt, ct, _callers = metric
        filename, lineno, func_name = func
        entries.append({
            "function": f"{filename}:{lineno}:{func_name}",
            "primitive_calls": int(cc),
            "total_calls": int(nc),
            "total_time_s": round(float(tt), 6),
            "cumulative_time_s": round(float(ct), 6),
        })
    return [{"text": raw_text}] + entries


def _run_iterations(
    *,
    old_graph: dict[str, Any],
    new_graph: dict[str, Any],
    warmup: int,
    iterations: int,
    heartbeat_s: int,
    profile: str,
    geometry_policy: str,
    guid_policy: str,
    matcher_policy: dict[str, dict[str, Any]] | None,
    with_cprofile: bool,
    cprofile_sort: str,
    cprofile_top: int,
) -> dict[str, Any]:
    progress_state: dict[str, Any] = {}

    for idx in range(warmup):
        print(f"[prepare] warmup {idx + 1}/{warmup} start", file=sys.stderr, flush=True)
        warm = _run_prepare_once(
            old_graph=old_graph,
            new_graph=new_graph,
            profile=profile,
            geometry_policy=geometry_policy,
            guid_policy=guid_policy,
            matcher_policy=matcher_policy,
            progress_state=progress_state,
            cprof=None,
        )
        print(
            f"[prepare] warmup {idx + 1}/{warmup} done elapsed={_format_duration_ms(warm.elapsed_ms)} peak_bytes={warm.peak_mem_bytes}",
            file=sys.stderr,
            flush=True,
        )

    time_samples_ms: list[float] = []
    peak_samples_bytes: list[int] = []
    timing_samples: list[dict[str, float]] = []
    signatures: list[dict[str, Any]] = []
    cprofile_payload: list[dict[str, Any]] | None = None

    for idx in range(iterations):
        print(f"[prepare] iter {idx + 1}/{iterations} start", file=sys.stderr, flush=True)
        t0 = time.perf_counter()
        stop = threading.Event()
        profile_this_iter = with_cprofile and idx == 0

        def _heartbeat() -> None:
            while not stop.wait(timeout=float(heartbeat_s)):
                elapsed_ms = (time.perf_counter() - t0) * 1000.0
                completed = progress_state.get("completed_steps")
                total = progress_state.get("total_steps")
                step = progress_state.get("step")
                status = progress_state.get("status")
                suffix = ""
                if isinstance(completed, int) and isinstance(total, int) and total > 0:
                    progress = min(max(completed / total, 0.0), 1.0)
                    eta_text = "n/a"
                    if progress > 0.0 and progress < 1.0:
                        elapsed_s = elapsed_ms / 1000.0
                        eta_text = _format_eta((elapsed_s * (1.0 - progress)) / progress)
                    suffix += f" progress~{progress * 100.0:.1f}% eta~{eta_text}"
                    suffix += f" steps={completed}/{total}"
                if isinstance(step, str):
                    suffix += f" step={step}"
                if isinstance(status, str):
                    suffix += f" status={status}"
                print(
                    f"[prepare] iter {idx + 1}/{iterations} heartbeat elapsed={_format_duration_ms(elapsed_ms)}{suffix}",
                    file=sys.stderr,
                    flush=True,
                )

        heartbeat_thread: threading.Thread | None = None
        if heartbeat_s > 0 and not profile_this_iter:
            heartbeat_thread = threading.Thread(target=_heartbeat, daemon=True)
            heartbeat_thread.start()

        profiler: cProfile.Profile | None = None
        if profile_this_iter:
            profiler = cProfile.Profile()

        result = _run_prepare_once(
            old_graph=old_graph,
            new_graph=new_graph,
            profile=profile,
            geometry_policy=geometry_policy,
            guid_policy=guid_policy,
            matcher_policy=matcher_policy,
            progress_state=progress_state,
            cprof=profiler,
        )
        stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=1.0)

        if profiler is not None:
            cprofile_payload = _profile_top(profiler, sort_by=cprofile_sort, top_n=cprofile_top)

        print(
            f"[prepare] iter {idx + 1}/{iterations} done elapsed={_format_duration_ms(result.elapsed_ms)} peak_bytes={result.peak_mem_bytes}",
            file=sys.stderr,
            flush=True,
        )
        top_stages = sorted(result.timings_ms.items(), key=lambda kv: kv[1], reverse=True)[:10]
        for key, value in top_stages:
            print(
                f"[prepare] iter {idx + 1}/{iterations} stage={key} elapsed={_format_duration_ms(value)}",
                file=sys.stderr,
                flush=True,
            )

        time_samples_ms.append(result.elapsed_ms)
        peak_samples_bytes.append(result.peak_mem_bytes)
        timing_samples.append(result.timings_ms)
        signatures.append(result.signature)

    stable = all(sig == signatures[0] for sig in signatures[1:]) if signatures else True
    payload: dict[str, Any] = {
        "samples": {
            "time_ms": [round(v, 3) for v in time_samples_ms],
            "peak_mem_bytes": peak_samples_bytes,
        },
        "summary": {
            "time_ms": _summarize_float_samples(time_samples_ms),
            "peak_mem_bytes": _summarize_int_samples(peak_samples_bytes),
        },
        "stable_output_signature": stable,
        "output_signature": signatures[0] if signatures else {},
        "timings_ms": _summarize_named_float_samples(timing_samples),
    }
    if cprofile_payload is not None:
        payload["cprofile_top"] = cprofile_payload
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Profile prepare_diff_context only.")
    parser.add_argument("--old", required=True, help="Path to old IFC file")
    parser.add_argument("--new", help="Path to new IFC file (default: same as --old)")
    parser.add_argument("--profile", choices=SUPPORTED_PROFILES, default=DEFAULT_PROFILE)
    parser.add_argument("--guid-policy", choices=GUID_POLICY_CHOICES, default=GUID_POLICY_FAIL_FAST)
    parser.add_argument("--geometry-policy", choices=GEOMETRY_POLICY_CHOICES, default=GEOMETRY_POLICY_STRICT_SYNTAX)
    parser.add_argument("--matcher-policy-json", help="Inline JSON object for matcher policy overrides")
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--heartbeat-s", type=int, default=15)
    parser.add_argument("--cprofile", action="store_true", help="Capture cProfile top table for first measured iteration")
    parser.add_argument("--cprofile-sort", choices=("cumulative", "tottime"), default="cumulative")
    parser.add_argument("--cprofile-top", type=int, default=40)
    parser.add_argument("--out", required=True, help="Output JSON path")
    args = parser.parse_args()

    if args.warmup < 0:
        raise ValueError("--warmup must be >= 0")
    if args.iterations < 1:
        raise ValueError("--iterations must be >= 1")
    if args.heartbeat_s < 0:
        raise ValueError("--heartbeat-s must be >= 0")

    old_path = Path(args.old)
    new_path = Path(args.new) if args.new else old_path
    matcher_policy = _load_matcher_policy(args.matcher_policy_json)

    generated_at = datetime.now(timezone.utc).isoformat()
    run_started = time.perf_counter()

    print(f"[prepare] parsing old graph {old_path}", file=sys.stderr, flush=True)
    t0 = time.perf_counter()
    old_graph = parse_graph(str(old_path), profile=args.profile)
    parse_old_ms = (time.perf_counter() - t0) * 1000.0
    print(f"[prepare] parsed old graph in {_format_duration_ms(parse_old_ms)}", file=sys.stderr, flush=True)

    if old_path.resolve() == new_path.resolve():
        new_graph = old_graph
        parse_new_ms = 0.0
        print("[prepare] reusing old graph object for new graph (same path)", file=sys.stderr, flush=True)
    else:
        print(f"[prepare] parsing new graph {new_path}", file=sys.stderr, flush=True)
        t1 = time.perf_counter()
        new_graph = parse_graph(str(new_path), profile=args.profile)
        parse_new_ms = (time.perf_counter() - t1) * 1000.0
        print(f"[prepare] parsed new graph in {_format_duration_ms(parse_new_ms)}", file=sys.stderr, flush=True)

    prepare_stats = _run_iterations(
        old_graph=old_graph,
        new_graph=new_graph,
        warmup=args.warmup,
        iterations=args.iterations,
        heartbeat_s=args.heartbeat_s,
        profile=args.profile,
        geometry_policy=args.geometry_policy,
        guid_policy=args.guid_policy,
        matcher_policy=matcher_policy,
        with_cprofile=bool(args.cprofile),
        cprofile_sort=args.cprofile_sort,
        cprofile_top=args.cprofile_top,
    )

    total_elapsed_ms = (time.perf_counter() - run_started) * 1000.0
    payload = {
        "generated_at": generated_at,
        "config": {
            "old_path": str(old_path),
            "new_path": str(new_path),
            "profile": args.profile,
            "guid_policy": args.guid_policy,
            "geometry_policy": args.geometry_policy,
            "matcher_policy_json": matcher_policy,
            "warmup": args.warmup,
            "iterations": args.iterations,
            "heartbeat_s": args.heartbeat_s,
            "cprofile": bool(args.cprofile),
            "cprofile_sort": args.cprofile_sort,
            "cprofile_top": args.cprofile_top,
        },
        "parse_ms": {
            "old_graph": round(parse_old_ms, 3),
            "new_graph": round(parse_new_ms, 3),
            "total": round(parse_old_ms + parse_new_ms, 3),
        },
        "prepare_context": prepare_stats,
        "run_summary": {
            "total_elapsed_ms": round(total_elapsed_ms, 3),
            "total_elapsed_text": _format_duration_ms(total_elapsed_ms),
        },
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote prepare-context profile to {out_path}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
