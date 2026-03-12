from scripts.explore import render_perf_summary


def test_render_baseline_includes_engine_stage_timings_when_present():
    report = {
        "results": [
            {
                "case": {"name": "ifchouse"},
                "metrics": {
                    "diff_graphs": {
                        "summary": {
                            "time_ms": {"mean": 100.0},
                            "peak_mem_bytes": {"mean": 10},
                        },
                        "stable_output_signature": True,
                        "engine_timings_ms": {
                            "summary": {
                                "parse_old_graph": {"mean": 20.0},
                                "prepare_context": {"mean": 60.0},
                            }
                        },
                    },
                    "stream_diff_graphs_ndjson": {
                        "summary": {"time_ms": {"mean": 10.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                    "stream_diff_graphs_chunked_json": {
                        "summary": {"time_ms": {"mean": 10.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                },
            }
        ]
    }

    lines = render_perf_summary._render_baseline(report)
    text = "\n".join(lines)
    assert "### Diff Stage Timings (`diff_graphs`)" in text
    assert "| `ifchouse` | `prepare_context` | 60.00 ms |" in text
    assert text.index("`prepare_context`") < text.index("`parse_old_graph`")


def test_render_baseline_skips_engine_stage_timings_when_absent():
    report = {
        "results": [
            {
                "case": {"name": "ifchouse"},
                "metrics": {
                    "diff_graphs": {
                        "summary": {"time_ms": {"mean": 100.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                    "stream_diff_graphs_ndjson": {
                        "summary": {"time_ms": {"mean": 10.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                    "stream_diff_graphs_chunked_json": {
                        "summary": {"time_ms": {"mean": 10.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                },
            }
        ]
    }

    lines = render_perf_summary._render_baseline(report)
    assert "### Diff Stage Timings (`diff_graphs`)" not in "\n".join(lines)


def test_render_baseline_includes_parse_timings_when_present():
    report = {
        "results": [
            {
                "case": {"name": "ifchouse"},
                "parse_ms": {"old_graph": 12.0, "new_graph": 13.5, "total": 25.5},
                "metrics": {
                    "diff_graphs": {
                        "summary": {"time_ms": {"mean": 100.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                    "stream_diff_graphs_ndjson": {
                        "summary": {"time_ms": {"mean": 10.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                    "stream_diff_graphs_chunked_json": {
                        "summary": {"time_ms": {"mean": 10.0}, "peak_mem_bytes": {"mean": 10}},
                        "stable_output_signature": True,
                    },
                },
            }
        ]
    }

    text = "\n".join(render_perf_summary._render_baseline(report))
    assert "### Parse Timings" in text
    assert "| `ifchouse` | 12.00 ms | 13.50 ms | 25.50 ms |" in text


def test_render_suite_manifest_section():
    report = {
        "state": "running",
        "completed_steps": 2,
        "total_steps": 5,
        "current_step": {"index": 3, "name": "wl_benchmark"},
        "steps": [
            {"name": "baseline", "exit_code": 0, "timed_out": False, "elapsed_s": 12.345},
            {"name": "wl_benchmark", "exit_code": 124, "timed_out": True, "elapsed_s": 3600.0},
        ],
    }

    text = "\n".join(render_perf_summary._render_suite_manifest(report))
    assert "## Perf Suite Run" in text
    assert "- state: `running`" in text
    assert "- current step: `3/5` `wl_benchmark`" in text
    assert "| `baseline` | `0` | `False` | `12.345s` | no |" in text
    assert "| `wl_benchmark` | `124` | `True` | `3600.000s` | no |" in text


def test_render_suite_manifest_includes_current_probe_snapshot():
    report = {
        "state": "running",
        "completed_steps": 0,
        "total_steps": 7,
        "current_step": {
            "index": 1,
            "name": "baseline",
            "heartbeat": {
                "elapsed_s": 120.5,
                "probe": {
                    "state": "running",
                    "current_case": {
                        "name": "ifchouse",
                        "metric": "diff_graphs",
                        "stage": "emit_base_changes",
                        "progress_fraction": 0.61,
                        "eta_text": "4m 20s",
                    },
                },
            },
        },
        "steps": [],
    }

    text = "\n".join(render_perf_summary._render_suite_manifest(report))
    assert "- current heartbeat elapsed: `120.500s`" in text
    assert "- baseline probe state: `running`" in text
    assert "- baseline probe case: `ifchouse`" in text
    assert "- baseline probe metric: `diff_graphs`" in text
    assert "- baseline probe stage: `emit_base_changes`" in text
    assert "- baseline probe progress: `61.0%`" in text
    assert "- baseline probe eta: `4m 20s`" in text
