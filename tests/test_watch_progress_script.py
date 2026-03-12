from scripts.explore import watch_progress


def test_format_progress_line_running_snapshot():
    line = watch_progress._format_progress_line({
        "state": "running",
        "completed_cases": 0,
        "total_cases": 1,
        "current_case": {
            "name": "ifchouse",
            "metric": "diff_graphs",
            "phase": "metrics",
            "completed": 123,
            "total": 456,
            "bytes": 9999,
            "probe": {"stage": "emit_base_changes", "status": "running"},
            "progress_fraction": 0.42,
            "eta_text": "6m 30s",
        },
    })
    assert "state=running" in line
    assert "cases=0/1" in line
    assert "case=ifchouse" in line
    assert "metric=diff_graphs" in line
    assert "items=123/456" in line
    assert "bytes=9999" in line
    assert "stage=emit_base_changes" in line
    assert "progress~42.0%" in line
    assert "eta~6m 30s" in line


def test_format_progress_line_failed_snapshot():
    line = watch_progress._format_progress_line({
        "state": "failed",
        "completed_cases": 0,
        "total_cases": 1,
        "error": "boom",
    })
    assert "state=failed" in line
    assert "cases=0/1" in line
    assert "error=boom" in line
