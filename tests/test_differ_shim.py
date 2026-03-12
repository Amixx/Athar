from pathlib import Path

import pytest

import athar.differ as differ_mod


def test_differ_routes_path_inputs_to_diff_files(monkeypatch):
    called = {}

    def fake_diff_files(old_path, new_path, **kwargs):
        called["old_path"] = old_path
        called["new_path"] = new_path
        called["kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(differ_mod, "diff_files", fake_diff_files)

    result = differ_mod.diff(
        Path("/tmp/old.ifc"),
        Path("/tmp/new.ifc"),
        profile="semantic_stable",
        geometry_policy="invariant_probe",
        guid_policy="disambiguate",
        matcher_policy={"secondary_match": {"assignment_max": 8}},
        timings=True,
    )

    assert result == {"ok": True}
    assert called["old_path"] == "/tmp/old.ifc"
    assert called["new_path"] == "/tmp/new.ifc"
    assert called["kwargs"]["profile"] == "semantic_stable"
    assert called["kwargs"]["geometry_policy"] == "invariant_probe"
    assert called["kwargs"]["guid_policy"] == "disambiguate"
    assert called["kwargs"]["matcher_policy"] == {"secondary_match": {"assignment_max": 8}}
    assert called["kwargs"]["timings"] is True


def test_differ_routes_graph_inputs_to_diff_graphs(monkeypatch):
    called = {}

    def fake_diff_graphs(old_graph, new_graph, **kwargs):
        called["old_graph"] = old_graph
        called["new_graph"] = new_graph
        called["kwargs"] = kwargs
        return {"ok": True}

    monkeypatch.setattr(differ_mod, "diff_graphs", fake_diff_graphs)

    old_graph = {"entities": {1: {"entity_type": "IfcWall", "attributes": {}, "refs": []}}}
    new_graph = {"metadata": {"schema": "IFC4"}, "entities": {2: {"entity_type": "IfcWall", "attributes": {}, "refs": []}}}

    result = differ_mod.diff(old_graph, new_graph)

    assert result == {"ok": True}
    assert called["old_graph"]["metadata"]["schema"] == "IFC4"
    assert called["new_graph"]["metadata"]["schema"] == "IFC4"
    assert called["old_graph"]["entities"] is old_graph["entities"]
    assert called["new_graph"]["entities"] is new_graph["entities"]


def test_differ_rejects_mixed_path_and_graph_inputs():
    with pytest.raises(TypeError):
        differ_mod.diff("/tmp/old.ifc", {"metadata": {"schema": "IFC4"}, "entities": {}})
