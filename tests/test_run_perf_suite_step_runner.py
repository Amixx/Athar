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
