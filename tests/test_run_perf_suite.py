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
    assert manifest["steps"][0].get("resumed_skip") is not True
    assert manifest["steps"][0]["artifact_exists"] is True
