"""Render perf JSON artifacts into a concise markdown summary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _fmt_ms(val: Any) -> str:
    try:
        return f"{float(val):.2f} ms"
    except Exception:
        return "n/a"


def _fmt_mb(val: Any) -> str:
    try:
        return f"{(float(val) / (1024.0 * 1024.0)):.2f} MiB"
    except Exception:
        return "n/a"


def _render_baseline(report: dict[str, Any]) -> list[str]:
    lines = ["## Diff Baseline", "", "| Case | Metric | Mean Time | Mean Peak Mem | Stable Signature |", "|---|---:|---:|---:|---:|"]
    for case in report.get("results", []):
        name = case.get("case", {}).get("name", "unknown")
        metrics = case.get("metrics", {})
        for metric_name in ("diff_graphs", "stream_diff_graphs_ndjson", "stream_diff_graphs_chunked_json"):
            metric = metrics.get(metric_name, {})
            summary = metric.get("summary", {})
            time_mean = _fmt_ms(summary.get("time_ms", {}).get("mean"))
            mem_mean = _fmt_mb(summary.get("peak_mem_bytes", {}).get("mean"))
            stable = "yes" if metric.get("stable_output_signature") else "no"
            lines.append(f"| `{name}` | `{metric_name}` | {time_mean} | {mem_mean} | {stable} |")
    lines.extend(_render_baseline_parse_times(report))
    lines.extend(_render_baseline_engine_timings(report))
    lines.append("")
    return lines


def _render_baseline_parse_times(report: dict[str, Any]) -> list[str]:
    rows: list[tuple[str, Any, Any, Any]] = []
    for case in report.get("results", []):
        case_name = case.get("case", {}).get("name", "unknown")
        parse_ms = case.get("parse_ms", {})
        if not isinstance(parse_ms, dict):
            continue
        rows.append((
            case_name,
            parse_ms.get("old_graph"),
            parse_ms.get("new_graph"),
            parse_ms.get("total"),
        ))

    if not rows:
        return []

    rows.sort(key=lambda item: item[0])
    lines = [
        "",
        "### Parse Timings",
        "",
        "| Case | Parse Old | Parse New | Parse Total |",
        "|---|---:|---:|---:|",
    ]
    for case_name, old_ms, new_ms, total_ms in rows:
        lines.append(
            f"| `{case_name}` | {_fmt_ms(old_ms)} | {_fmt_ms(new_ms)} | {_fmt_ms(total_ms)} |"
        )
    return lines


def _render_baseline_engine_timings(report: dict[str, Any]) -> list[str]:
    rows: list[tuple[str, str, float]] = []
    for case in report.get("results", []):
        case_name = case.get("case", {}).get("name", "unknown")
        metric = case.get("metrics", {}).get("diff_graphs", {})
        summary = metric.get("engine_timings_ms", {}).get("summary", {})
        if not isinstance(summary, dict):
            continue
        for stage, stats in summary.items():
            if not isinstance(stats, dict):
                continue
            mean = stats.get("mean")
            if isinstance(mean, (int, float)):
                rows.append((case_name, str(stage), float(mean)))

    if not rows:
        return []

    rows.sort(key=lambda item: (-item[2], item[0], item[1]))
    lines = [
        "",
        "### Diff Stage Timings (`diff_graphs`)",
        "",
        "| Case | Stage | Mean Time |",
        "|---|---:|---:|",
    ]
    for case_name, stage, mean_ms in rows:
        lines.append(f"| `{case_name}` | `{stage}` | {mean_ms:.2f} ms |")
    return lines


def _render_matcher_quality(report: dict[str, Any]) -> list[str]:
    overall = report.get("overall", {})
    lines = [
        "## Matcher Quality",
        "",
        f"- scenarios: `{overall.get('scenarios', 'n/a')}`",
        f"- precision: `{overall.get('precision', 'n/a')}`",
        f"- recall: `{overall.get('recall', 'n/a')}`",
        f"- f1: `{overall.get('f1', 'n/a')}`",
        f"- exact-match scenarios: `{overall.get('exact_match_scenarios', 'n/a')}`",
        "",
    ]
    return lines


def _render_determinism_stress(report: dict[str, Any]) -> list[str]:
    lines = ["## Determinism Stress", ""]
    for key in ("diff_graphs", "stream_diff_graphs_ndjson", "stream_diff_graphs_chunked_json"):
        stable = report.get(key, {}).get("stable")
        lines.append(f"- `{key}` stable: `{stable}`")
    lines.append("")
    return lines


def _render_wl_benchmark(report: dict[str, Any]) -> list[str]:
    lines = [
        "## WL Backend Benchmark",
        "",
        "| Graph | Backend | Available | Mean Time | Mean Peak Mem | Stable Signature |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for graph in report.get("graphs", []):
        graph_path = graph.get("graph_path", "unknown")
        for backend, data in sorted(graph.get("backends", {}).items()):
            available = data.get("available")
            if not available:
                lines.append(f"| `{graph_path}` | `{backend}` | no | n/a | n/a | n/a |")
                continue
            summary = data.get("summary", {})
            mean_time = _fmt_ms(summary.get("time_ms", {}).get("mean"))
            mean_mem = _fmt_mb(summary.get("peak_mem_bytes", {}).get("mean"))
            stable = "yes" if data.get("stable_signature") else "no"
            lines.append(f"| `{graph_path}` | `{backend}` | yes | {mean_time} | {mean_mem} | {stable} |")
    lines.append("")
    return lines


def _render_wl_consistency(report: dict[str, Any]) -> list[str]:
    lines = [
        "## WL Backend Consistency",
        "",
        "| Graph | Backend | Available | Color Partition == sha256 | Class Partition == sha256 |",
        "|---|---:|---:|---:|---:|",
    ]
    for graph in report.get("graphs", []):
        graph_path = graph.get("graph_path", "unknown")
        consistency = graph.get("consistency", {})
        for backend, data in sorted(consistency.items()):
            available = data.get("available")
            if not available:
                lines.append(f"| `{graph_path}` | `{backend}` | no | n/a | n/a |")
                continue
            color_ok = "yes" if data.get("matches_sha256") else "no"
            class_ok = "yes" if data.get("class_partition_matches_sha256") else "no"
            lines.append(f"| `{graph_path}` | `{backend}` | yes | {color_ok} | {class_ok} |")
    lines.append("")
    return lines


def _render_owner_projection(report: dict[str, Any]) -> list[str]:
    memory = report.get("modes", {}).get("memory", {})
    disk = report.get("modes", {}).get("disk_spill", {})
    lines = [
        "## Owner Projection Benchmark",
        "",
        f"- case: `{report.get('config', {}).get('case', {}).get('name', 'unknown')}`",
        f"- equivalent output: `{report.get('equivalent_output')}`",
        "",
        "| Mode | Mean Time | Mean Peak Mem | Stable Signature |",
        "|---|---:|---:|---:|",
        (
            f"| `memory` | {_fmt_ms(memory.get('summary', {}).get('time_ms', {}).get('mean'))} "
            f"| {_fmt_mb(memory.get('summary', {}).get('peak_mem_bytes', {}).get('mean'))} "
            f"| {'yes' if memory.get('stable_output_signature') else 'no'} |"
        ),
        (
            f"| `disk_spill` | {_fmt_ms(disk.get('summary', {}).get('time_ms', {}).get('mean'))} "
            f"| {_fmt_mb(disk.get('summary', {}).get('peak_mem_bytes', {}).get('mean'))} "
            f"| {'yes' if disk.get('stable_output_signature') else 'no'} |"
        ),
        "",
    ]
    return lines


def _render_suite_manifest(report: dict[str, Any]) -> list[str]:
    lines = [
        "## Perf Suite Run",
        "",
        f"- state: `{report.get('state', 'unknown')}`",
        f"- completed steps: `{report.get('completed_steps', 'n/a')}/{report.get('total_steps', 'n/a')}`",
    ]
    current_step = report.get("current_step")
    if isinstance(current_step, dict):
        lines.append(
            f"- current step: `{current_step.get('index', '?')}/{report.get('total_steps', '?')}` "
            f"`{current_step.get('name', 'unknown')}`"
        )
        heartbeat = current_step.get("heartbeat")
        if isinstance(heartbeat, dict):
            elapsed_s = heartbeat.get("elapsed_s")
            if isinstance(elapsed_s, (int, float)):
                lines.append(f"- current heartbeat elapsed: `{float(elapsed_s):.3f}s`")
            probe = heartbeat.get("probe")
            if isinstance(probe, dict):
                probe_state = probe.get("state")
                probe_case = probe.get("current_case", {})
                if isinstance(probe_state, str):
                    lines.append(f"- baseline probe state: `{probe_state}`")
                if isinstance(probe_case, dict):
                    case_name = probe_case.get("name")
                    metric = probe_case.get("metric")
                    stage = probe_case.get("stage")
                    completed = probe_case.get("completed")
                    total = probe_case.get("total")
                    bytes_out = probe_case.get("bytes")
                    progress = probe_case.get("progress_fraction")
                    eta_text = probe_case.get("eta_text")
                    if isinstance(case_name, str):
                        lines.append(f"- baseline probe case: `{case_name}`")
                    if isinstance(metric, str):
                        lines.append(f"- baseline probe metric: `{metric}`")
                    if isinstance(stage, str):
                        lines.append(f"- baseline probe stage: `{stage}`")
                    if isinstance(completed, int) and isinstance(total, int) and total > 0:
                        lines.append(f"- baseline probe items: `{completed}/{total}`")
                    if isinstance(bytes_out, int):
                        lines.append(f"- baseline probe bytes: `{bytes_out}`")
                    if isinstance(progress, (int, float)):
                        lines.append(f"- baseline probe progress: `{float(progress) * 100.0:.1f}%`")
                    if isinstance(eta_text, str):
                        lines.append(f"- baseline probe eta: `{eta_text}`")
    lines.extend([
        "",
        "| Step | Exit | Timed Out | Elapsed | Resumed Skip |",
        "|---|---:|---:|---:|---:|",
    ])
    for step in report.get("steps", []):
        if not isinstance(step, dict):
            continue
        name = step.get("name", "unknown")
        exit_code = step.get("exit_code", "n/a")
        timed_out = step.get("timed_out", "n/a")
        elapsed = step.get("elapsed_s")
        resumed_skip = "yes" if step.get("resumed_skip") else "no"
        elapsed_text = f"{float(elapsed):.3f}s" if isinstance(elapsed, (int, float)) else "n/a"
        lines.append(f"| `{name}` | `{exit_code}` | `{timed_out}` | `{elapsed_text}` | {resumed_skip} |")
    lines.append("")
    return lines


def main() -> None:
    parser = argparse.ArgumentParser(description="Render perf JSON artifacts into markdown.")
    parser.add_argument("--baseline", default=None, help="Path to benchmark_diff_engine JSON report.")
    parser.add_argument("--wl-benchmark", default=None, help="Path to WL backend benchmark JSON report.")
    parser.add_argument("--wl-consistency", default=None, help="Path to WL backend consistency JSON report.")
    parser.add_argument("--owner-projection", default=None, help="Path to owner projection benchmark JSON report.")
    parser.add_argument("--matcher-quality", default=None, help="Path to matcher quality JSON report.")
    parser.add_argument("--determinism", default=None, help="Path to determinism stress JSON report.")
    parser.add_argument("--suite-manifest", default=None, help="Path to run_perf_suite manifest JSON report.")
    parser.add_argument("--out", default="docs/perf/SUMMARY.md", help="Output markdown path.")
    args = parser.parse_args()

    lines = ["# Perf Summary", ""]
    if args.baseline:
        lines.extend(_render_baseline(_load_json(Path(args.baseline))))
    if args.wl_benchmark:
        lines.extend(_render_wl_benchmark(_load_json(Path(args.wl_benchmark))))
    if args.wl_consistency:
        lines.extend(_render_wl_consistency(_load_json(Path(args.wl_consistency))))
    if args.owner_projection:
        lines.extend(_render_owner_projection(_load_json(Path(args.owner_projection))))
    if args.matcher_quality:
        lines.extend(_render_matcher_quality(_load_json(Path(args.matcher_quality))))
    if args.determinism:
        lines.extend(_render_determinism_stress(_load_json(Path(args.determinism))))
    if args.suite_manifest:
        lines.extend(_render_suite_manifest(_load_json(Path(args.suite_manifest))))
    if not (
        args.baseline
        or args.wl_benchmark
        or args.wl_consistency
        or args.owner_projection
        or args.matcher_quality
        or args.determinism
        or args.suite_manifest
    ):
        lines.append("_No input reports provided._")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"Wrote summary to {out_path}")


if __name__ == "__main__":
    main()
