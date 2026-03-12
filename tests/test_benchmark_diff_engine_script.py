import json

from scripts.explore import benchmark_diff_engine


def test_progress_eta_helper():
    assert benchmark_diff_engine._progress_eta(1000.0, None) is None
    assert benchmark_diff_engine._progress_eta(1000.0, 0.0) is None
    assert benchmark_diff_engine._progress_eta(2000.0, 10000.0) == ("20.0%", "8s")
    assert benchmark_diff_engine._progress_eta(15000.0, 10000.0) == ("99.0%", "0s")


def test_eta_format_helper():
    assert benchmark_diff_engine._format_eta(8.2) == "8s"
    assert benchmark_diff_engine._format_eta(125) == "2m 5s"
    assert benchmark_diff_engine._format_eta(3675) == "1h 1m 15s"


def test_benchmark_diff_engine_collects_engine_timings(monkeypatch, tmp_path):
    out = tmp_path / "bench.json"
    diff_timings_args: list[bool] = []

    monkeypatch.setattr(benchmark_diff_engine, "parse_graph", lambda _path, profile: {"entities": {}, "metadata": {"schema": "IFC4"}})

    def _fake_diff_graphs(
        _old,
        _new,
        *,
        profile: str,
        guid_policy: str,
        timings: bool = False,
        progress_callback=None,
    ):
        assert profile == "semantic_stable"
        assert guid_policy == "fail_fast"
        if progress_callback is not None:
            progress_callback({"stage": "prepare_context", "status": "done", "overall_progress": 0.5})
        diff_timings_args.append(timings)
        return {
            "base_changes": [],
            "derived_markers": [],
            "stats": {"timings_ms": {"prepare_context": 12.5, "total": 20.0}} if timings else {},
        }

    monkeypatch.setattr(benchmark_diff_engine, "diff_graphs", _fake_diff_graphs)
    monkeypatch.setattr(
        benchmark_diff_engine,
        "stream_diff_graphs",
        lambda *_args, **_kwargs: iter(['{"kind":"end"}']),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "benchmark_diff_engine",
            "--case",
            "one:old.ifc:new.ifc",
            "--warmup",
            "0",
            "--iterations",
            "1",
            "--engine-timings",
            "--out",
            str(out),
        ],
    )

    benchmark_diff_engine.main()
    report = json.loads(out.read_text(encoding="utf-8"))

    assert report["config"]["engine_timings"] is True
    assert report["config"]["heartbeat_s"] == 30
    parse_ms = report["results"][0]["parse_ms"]
    assert set(parse_ms) == {"old_graph", "new_graph", "total"}
    assert parse_ms["total"] >= 0.0
    metric = report["results"][0]["metrics"]["diff_graphs"]
    assert diff_timings_args == [True]
    assert metric["engine_timings_ms"]["samples"]["prepare_context"] == [12.5]
    assert metric["engine_timings_ms"]["samples"]["total"] == [20.0]


def test_benchmark_diff_engine_does_not_collect_timings_by_default(monkeypatch, tmp_path):
    out = tmp_path / "bench.json"
    diff_timings_args: list[bool] = []

    monkeypatch.setattr(benchmark_diff_engine, "parse_graph", lambda _path, profile: {"entities": {}, "metadata": {"schema": "IFC4"}})

    def _fake_diff_graphs(
        _old,
        _new,
        *,
        profile: str,
        guid_policy: str,
        timings: bool = False,
        progress_callback=None,
    ):
        diff_timings_args.append(timings)
        return {"base_changes": [], "derived_markers": [], "stats": {}}

    monkeypatch.setattr(benchmark_diff_engine, "diff_graphs", _fake_diff_graphs)
    monkeypatch.setattr(
        benchmark_diff_engine,
        "stream_diff_graphs",
        lambda *_args, **_kwargs: iter(['{"kind":"end"}']),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "benchmark_diff_engine",
            "--case",
            "one:old.ifc:new.ifc",
            "--warmup",
            "0",
            "--iterations",
            "1",
            "--out",
            str(out),
        ],
    )

    benchmark_diff_engine.main()
    report = json.loads(out.read_text(encoding="utf-8"))

    assert report["config"]["engine_timings"] is False
    assert report["config"]["heartbeat_s"] == 30
    parse_ms = report["results"][0]["parse_ms"]
    assert set(parse_ms) == {"old_graph", "new_graph", "total"}
    assert parse_ms["total"] >= 0.0
    metric = report["results"][0]["metrics"]["diff_graphs"]
    assert diff_timings_args == [False]
    assert "engine_timings_ms" not in metric


def test_benchmark_diff_engine_writes_progress_sidecar(monkeypatch, tmp_path):
    out = tmp_path / "bench.json"
    progress = tmp_path / "progress.json"

    monkeypatch.setattr(
        benchmark_diff_engine,
        "parse_graph",
        lambda _path, profile: {"entities": {}, "metadata": {"schema": "IFC4"}},
    )
    monkeypatch.setattr(
        benchmark_diff_engine,
        "diff_graphs",
        lambda *_args, **_kwargs: {"base_changes": [], "derived_markers": [], "stats": {}},
    )
    monkeypatch.setattr(
        benchmark_diff_engine,
        "stream_diff_graphs",
        lambda *_args, **_kwargs: iter(['{"kind":"end"}']),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "benchmark_diff_engine",
            "--case",
            "one:old.ifc:new.ifc",
            "--warmup",
            "0",
            "--iterations",
            "1",
            "--progress-file",
            str(progress),
            "--out",
            str(out),
        ],
    )

    benchmark_diff_engine.main()
    sidecar = json.loads(progress.read_text(encoding="utf-8"))
    assert sidecar["state"] == "completed"
    assert sidecar["completed_cases"] == 1
    assert sidecar["total_cases"] == 1
    assert sidecar["current_case"] is None
    assert sidecar["report_path"] == str(out)
