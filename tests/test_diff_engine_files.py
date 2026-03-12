import pytest

import athar.diff_engine as diff_engine


def _mock_cross_schema_parse(monkeypatch):
    """Monkeypatch parse helpers to return two graphs with different schemas."""
    graphs = iter([
        {"metadata": {"schema": "IFC4"}, "entities": {}},
        {"metadata": {"schema": "IFC2X3"}, "entities": {}},
    ])
    monkeypatch.setattr(diff_engine, "parse_graph", lambda *_args, **_kwargs: next(graphs))
    # Non-same-input path uses open_ifc + graph_from_ifc directly
    monkeypatch.setattr(diff_engine, "open_ifc", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(diff_engine, "graph_from_ifc", lambda *_args, **_kwargs: next(graphs))


def test_diff_files_rejects_cross_schema_pairs(monkeypatch):
    _mock_cross_schema_parse(monkeypatch)

    with pytest.raises(ValueError, match="Schema mismatch: IFC4 vs IFC2X3"):
        diff_engine.diff_files("old.ifc", "new.ifc")


def test_stream_diff_files_rejects_cross_schema_pairs(monkeypatch):
    _mock_cross_schema_parse(monkeypatch)

    with pytest.raises(ValueError, match="Schema mismatch: IFC4 vs IFC2X3"):
        list(diff_engine.stream_diff_files("old.ifc", "new.ifc"))


def test_diff_files_parses_once_when_paths_are_same(monkeypatch):
    calls: list[str] = []
    graph = {"metadata": {"schema": "IFC4"}, "entities": {}}

    def fake_parse_graph(path: str, **_kwargs):
        calls.append(path)
        return graph

    seen: dict[str, bool] = {}

    def fake_diff_graphs(old_graph, new_graph, **_kwargs):
        seen["same_object"] = old_graph is new_graph
        return {"base_changes": [], "derived_markers": []}

    monkeypatch.setattr(diff_engine, "parse_graph", fake_parse_graph)
    monkeypatch.setattr(diff_engine, "diff_graphs", fake_diff_graphs)

    result = diff_engine.diff_files("same.ifc", "same.ifc")

    assert len(calls) == 1
    assert seen["same_object"] is True
    assert result == {"base_changes": [], "derived_markers": []}


def test_diff_files_timings_parse_new_graph_zero_when_paths_are_same(monkeypatch):
    calls: list[str] = []
    graph = {"metadata": {"schema": "IFC4"}, "entities": {}}

    def fake_parse_graph(path: str, **_kwargs):
        calls.append(path)
        return graph

    def fake_diff_graphs(_old_graph, _new_graph, **kwargs):
        return {"stats": {"timings_ms": kwargs["parse_timings_ms"]}, "base_changes": [], "derived_markers": []}

    monkeypatch.setattr(diff_engine, "parse_graph", fake_parse_graph)
    monkeypatch.setattr(diff_engine, "diff_graphs", fake_diff_graphs)

    result = diff_engine.diff_files("same.ifc", "same.ifc", timings=True)
    timings = result["stats"]["timings_ms"]
    assert len(calls) == 1
    assert timings["parse_new_graph"] == 0.0


def test_stream_diff_files_parses_once_when_paths_are_same(monkeypatch):
    calls: list[str] = []
    graph = {"metadata": {"schema": "IFC4"}, "entities": {}}

    def fake_parse_graph(path: str, **_kwargs):
        calls.append(path)
        return graph

    seen: dict[str, bool] = {}

    def fake_stream_diff_graphs(old_graph, new_graph, **_kwargs):
        seen["same_object"] = old_graph is new_graph
        yield "ok"

    monkeypatch.setattr(diff_engine, "parse_graph", fake_parse_graph)
    monkeypatch.setattr(diff_engine, "stream_diff_graphs", fake_stream_diff_graphs)

    out = list(diff_engine.stream_diff_files("same.ifc", "same.ifc"))

    assert len(calls) == 1
    assert seen["same_object"] is True
    assert out == ["ok"]
