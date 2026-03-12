"""Run the full perf harness suite sequentially and render a summary."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Callable


def _default_tag() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _run_step(
    *,
    name: str,
    cmd: list[str],
    artifact: Path | None,
    fail_fast: bool,
    timeout_s: int,
    heartbeat_s: int,
    heartbeat_probe: Callable[[], dict[str, Any] | None] | None = None,
    heartbeat_callback: Callable[[dict[str, Any]], None] | None = None,
    step_index: int,
    total_steps: int,
) -> dict[str, Any]:
    print(f"[suite] start step {step_index}/{total_steps} {name}: {' '.join(cmd)}", flush=True)
    started = time.perf_counter()
    timed_out = False
    exit_code = 0
    process = subprocess.Popen(cmd)
    next_heartbeat = started + float(heartbeat_s) if heartbeat_s > 0 else None
    last_probe_summary: dict[str, Any] | None = None
    while True:
        polled = process.poll()
        if polled is not None:
            exit_code = polled
            break
        now = time.perf_counter()
        elapsed = now - started
        if timeout_s > 0 and elapsed >= timeout_s:
            timed_out = True
            exit_code = 124
            print(
                f"[suite] timeout step {step_index}/{total_steps} {name} after {round(elapsed, 3)}s; terminating",
                flush=True,
            )
            process.kill()
            process.wait()
            break
        if next_heartbeat is not None and now >= next_heartbeat:
            raw_snapshot = None
            if heartbeat_probe is not None:
                raw_snapshot = heartbeat_probe()
            last_probe_summary = _summarize_probe_snapshot(raw_snapshot)
            detail = _format_heartbeat_snapshot(raw_snapshot)
            print(
                f"[suite] heartbeat step {step_index}/{total_steps} {name} elapsed={round(elapsed, 1)}s"
                f"{' ' + detail if detail else ''}",
                flush=True,
            )
            if heartbeat_callback is not None:
                heartbeat_callback({
                    "elapsed_s": round(elapsed, 3),
                    "probe": last_probe_summary,
                })
            next_heartbeat = now + float(heartbeat_s)
        time.sleep(1.0)
    elapsed_s = round(time.perf_counter() - started, 3)
    record: dict[str, Any] = {
        "name": name,
        "command": cmd,
        "exit_code": exit_code,
        "elapsed_s": elapsed_s,
        "timed_out": timed_out,
    }
    if artifact is not None:
        record["artifact"] = str(artifact)
        record["artifact_exists"] = artifact.exists()
    if last_probe_summary is not None:
        record["last_probe"] = last_probe_summary
    print(
        f"[suite] done step {step_index}/{total_steps} {name}: exit={exit_code} timed_out={timed_out} elapsed={elapsed_s}s",
        flush=True,
    )
    if (exit_code != 0 or timed_out) and fail_fast:
        raise RuntimeError(f"Step {name} failed with exit code {exit_code} (timed_out={timed_out})")
    return record


def _append_optional_path(cmd: list[str], flag: str, path: Path) -> None:
    if path.exists():
        cmd.extend([flag, str(path)])


def _load_json_snapshot(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _format_heartbeat_probe(probe: Callable[[], dict[str, Any] | None] | None) -> str:
    if probe is None:
        return ""
    try:
        snapshot = probe()
    except Exception:
        return ""
    return _format_heartbeat_snapshot(snapshot)


def _format_heartbeat_snapshot(snapshot: dict[str, Any] | None) -> str:
    if not isinstance(snapshot, dict):
        return ""
    parts: list[str] = []
    state = snapshot.get("state")
    if isinstance(state, str):
        parts.append(f"state={state}")
    current_case = snapshot.get("current_case")
    if isinstance(current_case, dict):
        name = current_case.get("name")
        metric = current_case.get("metric")
        phase = current_case.get("phase")
        completed = current_case.get("completed")
        total = current_case.get("total")
        probe_payload = current_case.get("probe")
        progress_fraction = current_case.get("progress_fraction")
        eta_text = current_case.get("eta_text")
        if isinstance(name, str):
            parts.append(f"case={name}")
        if isinstance(metric, str):
            parts.append(f"metric={metric}")
        if isinstance(phase, str):
            parts.append(f"phase={phase}")
        if isinstance(completed, int) and isinstance(total, int) and total > 0:
            parts.append(f"items={completed}/{total}")
        if isinstance(probe_payload, dict):
            stage = probe_payload.get("stage")
            status = probe_payload.get("status")
            if isinstance(stage, str):
                parts.append(f"stage={stage}")
            if isinstance(status, str):
                parts.append(f"status={status}")
        if isinstance(progress_fraction, (int, float)):
            parts.append(f"progress~{float(progress_fraction) * 100.0:.1f}%")
        if isinstance(eta_text, str):
            parts.append(f"eta~{eta_text}")
    return " ".join(parts)


def _summarize_probe_snapshot(snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(snapshot, dict):
        return None
    out: dict[str, Any] = {}
    state = snapshot.get("state")
    if isinstance(state, str):
        out["state"] = state
    current_case = snapshot.get("current_case")
    if isinstance(current_case, dict):
        cc: dict[str, Any] = {}
        for key in (
            "index",
            "total",
            "completed",
            "bytes",
            "name",
            "phase",
            "metric",
            "event",
            "status",
            "iteration",
            "iterations",
            "elapsed_ms",
            "progress_fraction",
            "eta_seconds",
            "eta_text",
        ):
            value = current_case.get(key)
            if value is not None:
                cc[key] = value
        probe_payload = current_case.get("probe")
        if isinstance(probe_payload, dict):
            stage = probe_payload.get("stage")
            status = probe_payload.get("status")
            if stage is not None:
                cc["stage"] = stage
            if status is not None:
                cc["stage_status"] = status
        out["current_case"] = cc
    completed = snapshot.get("completed_cases")
    total = snapshot.get("total_cases")
    if isinstance(completed, int):
        out["completed_cases"] = completed
    if isinstance(total, int):
        out["total_cases"] = total
    return out


def _load_previous_steps(manifest_path: Path) -> dict[str, dict[str, Any]]:
    if not manifest_path.exists():
        return {}
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    previous: dict[str, dict[str, Any]] = {}
    for step in manifest.get("steps", []):
        if isinstance(step, dict):
            name = step.get("name")
            if isinstance(name, str):
                previous[name] = step
    return previous


def _step_completed_successfully(previous: dict[str, Any], artifact: Path | None) -> bool:
    if previous.get("exit_code") != 0 or previous.get("timed_out"):
        return False
    if artifact is not None and not artifact.exists():
        return False
    return True


def _write_manifest(
    *,
    manifest_path: Path,
    tag: str,
    out_dir: Path,
    started_at: str,
    steps: list[dict[str, Any]],
    total_steps: int,
    state: str,
    current_step: dict[str, Any] | None,
) -> None:
    manifest = {
        "started_at": started_at,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "tag": tag,
        "out_dir": str(out_dir),
        "completed_steps": len(steps),
        "total_steps": total_steps,
        "state": state,
        "steps": steps,
    }
    if current_step is not None:
        manifest["current_step"] = current_step
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run perf harnesses and render a summary.")
    parser.add_argument("--out-dir", default="docs/perf", help="Output directory for reports.")
    parser.add_argument("--tag", default=None, help="Output tag suffix (default: UTC YYYY-MM-DD).")
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="Optional baseline case NAME:OLD_PATH:NEW_PATH (repeatable).",
    )
    parser.add_argument("--baseline-warmup", type=int, default=1)
    parser.add_argument("--baseline-iterations", type=int, default=2)
    parser.add_argument(
        "--baseline-engine-timings",
        action="store_true",
        help="Enable `benchmark_diff_engine --engine-timings` for per-stage diff_graphs timing breakdowns.",
    )
    parser.add_argument(
        "--baseline-progress-file",
        default=None,
        help="Optional path for `benchmark_diff_engine --progress-file` live sidecar JSON.",
    )
    parser.add_argument("--wl-warmup", type=int, default=1)
    parser.add_argument("--wl-iterations", type=int, default=2)
    parser.add_argument(
        "--wl-graph",
        action="append",
        default=[],
        help="Optional graph path for WL benchmark step (repeatable).",
    )
    parser.add_argument(
        "--wl-consistency-graph",
        action="append",
        default=[],
        help="Optional graph path for WL consistency step (repeatable).",
    )
    parser.add_argument("--owner-warmup", type=int, default=1)
    parser.add_argument("--owner-iterations", type=int, default=2)
    parser.add_argument(
        "--owner-case",
        default=None,
        help="Optional owner benchmark case NAME:OLD_PATH:NEW_PATH.",
    )
    parser.add_argument("--owner-disk-threshold", type=int, default=1)
    parser.add_argument("--determinism-rounds", type=int, default=25)
    parser.add_argument("--skip-baseline", action="store_true")
    parser.add_argument("--skip-wl", action="store_true")
    parser.add_argument("--skip-wl-consistency", action="store_true")
    parser.add_argument("--skip-owner-benchmark", action="store_true")
    parser.add_argument("--skip-matcher-quality", action="store_true")
    parser.add_argument("--skip-determinism", action="store_true")
    parser.add_argument("--skip-summary", action="store_true")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from an existing perf suite manifest and skip successful completed steps.",
    )
    parser.add_argument(
        "--step-timeout-s",
        type=int,
        default=0,
        help="Optional timeout in seconds per step (0 disables timeout).",
    )
    parser.add_argument(
        "--heartbeat-s",
        type=int,
        default=30,
        help="Optional suite-level heartbeat interval in seconds while a step is running (0 disables).",
    )
    parser.add_argument("--fail-fast", action="store_true")
    args = parser.parse_args()

    if args.baseline_warmup < 0:
        raise ValueError("--baseline-warmup must be >= 0")
    if args.baseline_iterations < 1:
        raise ValueError("--baseline-iterations must be >= 1")
    if args.wl_warmup < 0:
        raise ValueError("--wl-warmup must be >= 0")
    if args.wl_iterations < 1:
        raise ValueError("--wl-iterations must be >= 1")
    if args.owner_warmup < 0:
        raise ValueError("--owner-warmup must be >= 0")
    if args.owner_iterations < 1:
        raise ValueError("--owner-iterations must be >= 1")
    if args.determinism_rounds < 1:
        raise ValueError("--determinism-rounds must be >= 1")
    if args.step_timeout_s < 0:
        raise ValueError("--step-timeout-s must be >= 0")
    if args.heartbeat_s < 0:
        raise ValueError("--heartbeat-s must be >= 0")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = args.tag or _default_tag()

    baseline_path = out_dir / f"batch11_baseline_{tag}.json"
    wl_path = out_dir / f"wl_backend_benchmark_{tag}.json"
    wl_consistency_path = out_dir / f"wl_backend_consistency_{tag}.json"
    owner_benchmark_path = out_dir / f"owner_projection_benchmark_{tag}.json"
    matcher_quality_path = out_dir / f"matcher_quality_{tag}.json"
    determinism_path = out_dir / f"determinism_stress_{tag}.json"
    summary_path = out_dir / "SUMMARY.md"
    manifest_path = out_dir / f"perf_suite_run_{tag}.json"

    step_specs: list[tuple[str, list[str], Path | None]] = []

    if not args.skip_baseline:
        cmd = [
            sys.executable,
            "-m",
            "scripts.explore.benchmark_diff_engine",
            "--warmup",
            str(args.baseline_warmup),
            "--iterations",
            str(args.baseline_iterations),
            "--out",
            str(baseline_path),
        ]
        if args.baseline_engine_timings:
            cmd.append("--engine-timings")
        if args.baseline_progress_file:
            cmd.extend(["--progress-file", args.baseline_progress_file])
        for case in args.case:
            cmd.extend(["--case", case])
        step_specs.append(("baseline", cmd, baseline_path))

    if not args.skip_wl:
        wl_cmd = [
            sys.executable,
            "-m",
            "scripts.explore.benchmark_wl_backends",
            "--warmup",
            str(args.wl_warmup),
            "--iterations",
            str(args.wl_iterations),
            "--out",
            str(wl_path),
        ]
        for graph in args.wl_graph:
            wl_cmd.extend(["--graph", graph])
        step_specs.append(("wl_benchmark", wl_cmd, wl_path))

    if not args.skip_wl_consistency:
        wl_consistency_cmd = [
            sys.executable,
            "-m",
            "scripts.explore.check_wl_backend_consistency",
            "--out",
            str(wl_consistency_path),
        ]
        for graph in args.wl_consistency_graph:
            wl_consistency_cmd.extend(["--graph", graph])
        step_specs.append(("wl_consistency", wl_consistency_cmd, wl_consistency_path))

    if not args.skip_owner_benchmark:
        owner_cmd = [
            sys.executable,
            "-m",
            "scripts.explore.benchmark_owner_projection",
            "--warmup",
            str(args.owner_warmup),
            "--iterations",
            str(args.owner_iterations),
            "--disk-threshold",
            str(max(0, args.owner_disk_threshold)),
            "--out",
            str(owner_benchmark_path),
        ]
        if args.owner_case:
            owner_cmd.extend(["--case", args.owner_case])
        step_specs.append(("owner_projection_benchmark", owner_cmd, owner_benchmark_path))

    if not args.skip_matcher_quality:
        step_specs.append((
            "matcher_quality",
            [
                sys.executable,
                "-m",
                "scripts.explore.evaluate_matcher_quality",
                "--out",
                str(matcher_quality_path),
            ],
            matcher_quality_path,
        ))

    if not args.skip_determinism:
        step_specs.append((
            "determinism_stress",
            [
                sys.executable,
                "-m",
                "scripts.explore.stress_determinism",
                "--rounds",
                str(args.determinism_rounds),
                "--out",
                str(determinism_path),
            ],
            determinism_path,
        ))

    if not args.skip_summary:
        summary_cmd = [
            sys.executable,
            "-m",
            "scripts.explore.render_perf_summary",
            "--out",
            str(summary_path),
        ]
        _append_optional_path(summary_cmd, "--baseline", baseline_path)
        _append_optional_path(summary_cmd, "--wl-benchmark", wl_path)
        _append_optional_path(summary_cmd, "--wl-consistency", wl_consistency_path)
        _append_optional_path(summary_cmd, "--owner-projection", owner_benchmark_path)
        _append_optional_path(summary_cmd, "--matcher-quality", matcher_quality_path)
        _append_optional_path(summary_cmd, "--determinism", determinism_path)
        summary_cmd.extend(["--suite-manifest", str(manifest_path)])

        step_specs.append(("summary", summary_cmd, summary_path))

    steps: list[dict[str, Any]] = []
    started_at = datetime.now(timezone.utc).isoformat()
    total_steps = len(step_specs)
    previous_steps = _load_previous_steps(manifest_path) if args.resume else {}
    if args.resume and previous_steps:
        print(f"[suite] resume enabled: loaded {len(previous_steps)} prior step records from {manifest_path}")

    _write_manifest(
        manifest_path=manifest_path,
        tag=tag,
        out_dir=out_dir,
        started_at=started_at,
        steps=steps,
        total_steps=total_steps,
        state="running",
        current_step=None,
    )

    current_step: dict[str, Any] | None = None
    try:
        for idx, (name, cmd, artifact) in enumerate(step_specs, start=1):
            current_step = {
                "index": idx,
                "name": name,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            heartbeat_probe = None
            if name == "baseline" and args.baseline_progress_file:
                baseline_progress_path = Path(args.baseline_progress_file)
                current_step["baseline_progress_file"] = str(baseline_progress_path)
                heartbeat_probe = lambda path=baseline_progress_path: _load_json_snapshot(path)

            _write_manifest(
                manifest_path=manifest_path,
                tag=tag,
                out_dir=out_dir,
                started_at=started_at,
                steps=steps,
                total_steps=total_steps,
                state="running",
                current_step=current_step,
            )
            previous = previous_steps.get(name)
            if previous is not None and _step_completed_successfully(previous, artifact):
                record = dict(previous)
                record["name"] = name
                record["command"] = cmd
                record["resumed_skip"] = True
                record["resumed_at"] = datetime.now(timezone.utc).isoformat()
                if artifact is not None:
                    record["artifact"] = str(artifact)
                    record["artifact_exists"] = artifact.exists()
                print(f"[suite] skip step {idx}/{total_steps} {name}: already completed successfully")
                steps.append(record)
                current_step = None
                _write_manifest(
                    manifest_path=manifest_path,
                    tag=tag,
                    out_dir=out_dir,
                    started_at=started_at,
                    steps=steps,
                    total_steps=total_steps,
                    state="running",
                    current_step=current_step,
                )
                continue

            def _on_step_heartbeat(payload: dict[str, Any]) -> None:
                if current_step is None:
                    return
                current_step["heartbeat"] = payload
                _write_manifest(
                    manifest_path=manifest_path,
                    tag=tag,
                    out_dir=out_dir,
                    started_at=started_at,
                    steps=steps,
                    total_steps=total_steps,
                    state="running",
                    current_step=current_step,
                )

            steps.append(_run_step(
                name=name,
                cmd=cmd,
                artifact=artifact,
                fail_fast=args.fail_fast,
                timeout_s=args.step_timeout_s,
                heartbeat_s=args.heartbeat_s,
                heartbeat_probe=heartbeat_probe,
                heartbeat_callback=_on_step_heartbeat,
                step_index=idx,
                total_steps=total_steps,
            ))
            current_step = None
            _write_manifest(
                manifest_path=manifest_path,
                tag=tag,
                out_dir=out_dir,
                started_at=started_at,
                steps=steps,
                total_steps=total_steps,
                state="running",
                current_step=current_step,
            )
    except Exception:
        _write_manifest(
            manifest_path=manifest_path,
            tag=tag,
            out_dir=out_dir,
            started_at=started_at,
            steps=steps,
            total_steps=total_steps,
            state="failed",
            current_step=current_step,
        )
        raise

    _write_manifest(
        manifest_path=manifest_path,
        tag=tag,
        out_dir=out_dir,
        started_at=started_at,
        steps=steps,
        total_steps=total_steps,
        state="completed",
        current_step=None,
    )
    print(f"Wrote perf suite manifest to {manifest_path}")


if __name__ == "__main__":
    main()
