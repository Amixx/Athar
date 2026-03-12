"""Run the full perf harness suite sequentially and render a summary."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
from typing import Any


def _default_tag() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _run_step(
    *,
    name: str,
    cmd: list[str],
    artifact: Path | None,
    fail_fast: bool,
    timeout_s: int,
    step_index: int,
    total_steps: int,
) -> dict[str, Any]:
    print(f"[suite] start step {step_index}/{total_steps} {name}: {' '.join(cmd)}", flush=True)
    started = time.perf_counter()
    timed_out = False
    exit_code = 0
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            timeout=timeout_s if timeout_s > 0 else None,
        )
        exit_code = completed.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        exit_code = 124
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
        "--step-timeout-s",
        type=int,
        default=0,
        help="Optional timeout in seconds per step (0 disables timeout).",
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

        step_specs.append(("summary", summary_cmd, summary_path))

    steps: list[dict[str, Any]] = []
    total_steps = len(step_specs)
    for idx, (name, cmd, artifact) in enumerate(step_specs, start=1):
        steps.append(_run_step(
            name=name,
            cmd=cmd,
            artifact=artifact,
            fail_fast=args.fail_fast,
            timeout_s=args.step_timeout_s,
            step_index=idx,
            total_steps=total_steps,
        ))

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tag": tag,
        "out_dir": str(out_dir),
        "steps": steps,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Wrote perf suite manifest to {manifest_path}")


if __name__ == "__main__":
    main()
