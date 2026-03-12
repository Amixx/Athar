import sys

from scripts.explore import run_perf_suite


def test_run_step_success():
    record = run_perf_suite._run_step(
        name="ok",
        cmd=[sys.executable, "-c", "print('ok')"],
        artifact=None,
        fail_fast=False,
        timeout_s=0,
        heartbeat_s=0,
        step_index=1,
        total_steps=1,
    )

    assert record["name"] == "ok"
    assert record["exit_code"] == 0
    assert record["timed_out"] is False
    assert record["elapsed_s"] >= 0.0


def test_run_step_timeout():
    record = run_perf_suite._run_step(
        name="timeout",
        cmd=[sys.executable, "-c", "import time; time.sleep(2)"],
        artifact=None,
        fail_fast=False,
        timeout_s=1,
        heartbeat_s=0,
        step_index=1,
        total_steps=1,
    )

    assert record["name"] == "timeout"
    assert record["exit_code"] == 124
    assert record["timed_out"] is True


def test_run_step_heartbeat_callback_and_probe_summary():
    heartbeats: list[dict] = []
    record = run_perf_suite._run_step(
        name="heartbeat",
        cmd=[sys.executable, "-c", "import time; time.sleep(1.2)"],
        artifact=None,
        fail_fast=False,
        timeout_s=0,
        heartbeat_s=1,
        heartbeat_probe=lambda: {
            "state": "running",
            "current_case": {
                "name": "ifchouse",
                "metric": "diff_graphs",
                "phase": "metrics",
                "probe": {"stage": "emit_base_changes", "status": "running"},
                "progress_fraction": 0.5,
                "eta_text": "3m 0s",
            },
        },
        heartbeat_callback=lambda payload: heartbeats.append(payload),
        step_index=1,
        total_steps=1,
    )

    assert record["exit_code"] == 0
    assert len(heartbeats) >= 1
    assert "last_probe" in record
    assert record["last_probe"]["state"] == "running"
    assert record["last_probe"]["current_case"]["metric"] == "diff_graphs"
