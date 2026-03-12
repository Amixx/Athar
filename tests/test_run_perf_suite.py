import json
from pathlib import Path

from scripts.explore import run_perf_suite


def _load_manifest(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_run_perf_suite_resume_skips_successful_step(monkeypatch, tmp_path):
    out_dir = tmp_path / "perf"
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = "resume-ok"
    baseline = out_dir / f"batch11_baseline_{tag}.json"
    baseline.write_text("{}", encoding="utf-8")
    manifest_path = out_dir / f"perf_suite_run_{tag}.json"
    manifest_path.write_text(
        json.dumps({
            "steps": [
                {
                    "name": "baseline",
                    "command": ["python", "-m", "scripts.explore.benchmark_diff_engine"],
                    "exit_code": 0,
                    "timed_out": False,
                    "elapsed_s": 1.23,
                    "artifact": str(baseline),
                    "artifact_exists": True,
                }
            ]
        }),
        encoding="utf-8",
    )

    def _should_not_run_step(**_kwargs):
        raise AssertionError("baseline step should have been skipped via --resume")

    monkeypatch.setattr(run_perf_suite, "_run_step", _should_not_run_step)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_perf_suite",
            "--out-dir",
            str(out_dir),
            "--tag",
            tag,
            "--resume",
            "--skip-wl",
            "--skip-wl-consistency",
            "--skip-owner-benchmark",
            "--skip-matcher-quality",
            "--skip-determinism",
            "--skip-summary",
        ],
    )

    run_perf_suite.main()
    manifest = _load_manifest(manifest_path)
    assert manifest["completed_steps"] == 1
    assert manifest["total_steps"] == 1
    assert manifest["state"] == "completed"
    assert "current_step" not in manifest
    assert manifest["steps"][0]["name"] == "baseline"
    assert manifest["steps"][0]["resumed_skip"] is True


def test_run_perf_suite_resume_reruns_if_artifact_missing(monkeypatch, tmp_path):
    out_dir = tmp_path / "perf"
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = "resume-rerun"
    baseline = out_dir / f"batch11_baseline_{tag}.json"
    manifest_path = out_dir / f"perf_suite_run_{tag}.json"
    manifest_path.write_text(
        json.dumps({
            "steps": [
                {
                    "name": "baseline",
                    "command": ["python", "-m", "scripts.explore.benchmark_diff_engine"],
                    "exit_code": 0,
                    "timed_out": False,
                    "elapsed_s": 2.34,
                    "artifact": str(baseline),
                    "artifact_exists": False,
                }
            ]
        }),
        encoding="utf-8",
    )

    calls = {"count": 0}

    def _run_step_stub(**kwargs):
        calls["count"] += 1
        baseline.write_text("{}", encoding="utf-8")
        return {
            "name": kwargs["name"],
            "command": kwargs["cmd"],
            "exit_code": 0,
            "elapsed_s": 0.01,
            "timed_out": False,
            "artifact": str(baseline),
            "artifact_exists": True,
        }

    monkeypatch.setattr(run_perf_suite, "_run_step", _run_step_stub)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_perf_suite",
            "--out-dir",
            str(out_dir),
            "--tag",
            tag,
            "--resume",
            "--skip-wl",
            "--skip-wl-consistency",
            "--skip-owner-benchmark",
            "--skip-matcher-quality",
            "--skip-determinism",
            "--skip-summary",
        ],
    )

    run_perf_suite.main()
    manifest = _load_manifest(manifest_path)
    assert calls["count"] == 1
    assert manifest["completed_steps"] == 1
    assert manifest["steps"][0]["name"] == "baseline"
    assert manifest["state"] == "completed"
    assert "current_step" not in manifest
    assert manifest["steps"][0].get("resumed_skip") is not True
    assert manifest["steps"][0]["artifact_exists"] is True


def test_run_perf_suite_passes_baseline_engine_timings_flag(monkeypatch, tmp_path):
    out_dir = tmp_path / "perf"
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = "engine-timings"

    captured: dict[str, list[str]] = {}

    def _run_step_stub(**kwargs):
        captured["cmd"] = kwargs["cmd"]
        captured["heartbeat_probe"] = kwargs.get("heartbeat_probe")
        captured["heartbeat_callback"] = kwargs.get("heartbeat_callback")
        return {
            "name": kwargs["name"],
            "command": kwargs["cmd"],
            "exit_code": 0,
            "elapsed_s": 0.01,
            "timed_out": False,
            "artifact": kwargs["artifact"] and str(kwargs["artifact"]),
            "artifact_exists": False,
        }

    monkeypatch.setattr(run_perf_suite, "_run_step", _run_step_stub)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_perf_suite",
            "--out-dir",
            str(out_dir),
            "--tag",
            tag,
            "--baseline-engine-timings",
            "--skip-wl",
            "--skip-wl-consistency",
            "--skip-owner-benchmark",
            "--skip-matcher-quality",
            "--skip-determinism",
            "--skip-summary",
        ],
    )

    run_perf_suite.main()
    assert "--engine-timings" in captured["cmd"]
    manifest_path = out_dir / f"perf_suite_run_{tag}.json"
    manifest = _load_manifest(manifest_path)
    assert manifest["state"] == "completed"
    assert "current_step" not in manifest


def test_run_perf_suite_passes_heartbeat_interval_to_step_runner(monkeypatch, tmp_path):
    out_dir = tmp_path / "perf"
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = "heartbeat-forward"

    captured: dict[str, int] = {}

    def _run_step_stub(**kwargs):
        captured["heartbeat_s"] = kwargs["heartbeat_s"]
        return {
            "name": kwargs["name"],
            "command": kwargs["cmd"],
            "exit_code": 0,
            "elapsed_s": 0.01,
            "timed_out": False,
            "artifact": kwargs["artifact"] and str(kwargs["artifact"]),
            "artifact_exists": False,
        }

    monkeypatch.setattr(run_perf_suite, "_run_step", _run_step_stub)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_perf_suite",
            "--out-dir",
            str(out_dir),
            "--tag",
            tag,
            "--heartbeat-s",
            "7",
            "--skip-wl",
            "--skip-wl-consistency",
            "--skip-owner-benchmark",
            "--skip-matcher-quality",
            "--skip-determinism",
            "--skip-summary",
        ],
    )

    run_perf_suite.main()
    assert captured["heartbeat_s"] == 7


def test_run_perf_suite_summary_step_receives_suite_manifest_path(monkeypatch, tmp_path):
    out_dir = tmp_path / "perf"
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = "summary-manifest"
    captured: dict[str, list[str]] = {}

    def _run_step_stub(**kwargs):
        captured["cmd"] = kwargs["cmd"]
        captured["heartbeat_probe"] = kwargs.get("heartbeat_probe")
        captured["heartbeat_callback"] = kwargs.get("heartbeat_callback")
        return {
            "name": kwargs["name"],
            "command": kwargs["cmd"],
            "exit_code": 0,
            "elapsed_s": 0.01,
            "timed_out": False,
            "artifact": kwargs["artifact"] and str(kwargs["artifact"]),
            "artifact_exists": False,
        }

    monkeypatch.setattr(run_perf_suite, "_run_step", _run_step_stub)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_perf_suite",
            "--out-dir",
            str(out_dir),
            "--tag",
            tag,
            "--skip-baseline",
            "--skip-wl",
            "--skip-wl-consistency",
            "--skip-owner-benchmark",
            "--skip-matcher-quality",
            "--skip-determinism",
        ],
    )

    run_perf_suite.main()
    expected_manifest = str(out_dir / f"perf_suite_run_{tag}.json")
    assert "--suite-manifest" in captured["cmd"]
    assert expected_manifest in captured["cmd"]


def test_run_perf_suite_passes_baseline_progress_file(monkeypatch, tmp_path):
    out_dir = tmp_path / "perf"
    out_dir.mkdir(parents=True, exist_ok=True)
    tag = "baseline-progress-file"
    progress_file = tmp_path / "baseline-progress.json"
    captured: dict[str, list[str]] = {}

    def _run_step_stub(**kwargs):
        captured["cmd"] = kwargs["cmd"]
        captured["heartbeat_probe"] = kwargs.get("heartbeat_probe")
        captured["heartbeat_callback"] = kwargs.get("heartbeat_callback")
        return {
            "name": kwargs["name"],
            "command": kwargs["cmd"],
            "exit_code": 0,
            "elapsed_s": 0.01,
            "timed_out": False,
            "artifact": kwargs["artifact"] and str(kwargs["artifact"]),
            "artifact_exists": False,
        }

    monkeypatch.setattr(run_perf_suite, "_run_step", _run_step_stub)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_perf_suite",
            "--out-dir",
            str(out_dir),
            "--tag",
            tag,
            "--baseline-progress-file",
            str(progress_file),
            "--skip-wl",
            "--skip-wl-consistency",
            "--skip-owner-benchmark",
            "--skip-matcher-quality",
            "--skip-determinism",
            "--skip-summary",
        ],
    )

    run_perf_suite.main()
    assert "--progress-file" in captured["cmd"]
    assert str(progress_file) in captured["cmd"]
    assert callable(captured["heartbeat_probe"])
    assert callable(captured["heartbeat_callback"])


def test_format_heartbeat_probe_includes_nested_baseline_progress():
    detail = run_perf_suite._format_heartbeat_probe(
        lambda: {
            "state": "running",
            "current_case": {
                "name": "ifchouse",
                "metric": "diff_graphs",
                "phase": "metrics",
                "probe": {"stage": "emit_base_changes", "status": "running"},
                "progress_fraction": 0.756,
                "eta_text": "4m 12s",
            },
        }
    )
    assert "state=running" in detail
    assert "case=ifchouse" in detail
    assert "metric=diff_graphs" in detail
    assert "stage=emit_base_changes" in detail
    assert "progress~75.6%" in detail
    assert "eta~4m 12s" in detail


def test_summarize_probe_snapshot_extracts_current_case_fields():
    summary = run_perf_suite._summarize_probe_snapshot({
        "state": "running",
        "completed_cases": 0,
        "total_cases": 1,
        "current_case": {
            "name": "ifchouse",
            "metric": "diff_graphs",
            "phase": "metrics",
            "progress_fraction": 0.25,
            "eta_text": "8m 0s",
            "probe": {"stage": "emit_base_changes", "status": "running"},
        },
    })
    assert summary == {
        "state": "running",
        "completed_cases": 0,
        "total_cases": 1,
        "current_case": {
            "name": "ifchouse",
            "metric": "diff_graphs",
            "phase": "metrics",
            "progress_fraction": 0.25,
            "eta_text": "8m 0s",
            "stage": "emit_base_changes",
            "stage_status": "running",
        },
    }
