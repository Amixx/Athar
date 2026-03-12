from athar.diff.engine import diff_graphs
from athar.graph.graph_parser import parse_graph
from athar.graph.profile_policy import entity_for_profile


def test_entity_for_profile_raw_exact_keeps_owner_history_fields():
    entity = {
        "entity_type": "IfcWall",
        "attributes": {
            "Name": {"kind": "string", "value": "Wall A"},
            "OwnerHistory": {"kind": "ref", "id": 10},
        },
        "refs": [{"path": "/OwnerHistory", "target": 10, "target_type": "IfcOwnerHistory"}],
    }
    out = entity_for_profile(entity, profile="raw_exact")
    assert out == entity


def test_entity_for_profile_semantic_stable_filters_owner_history():
    entity = {
        "entity_type": "IfcWall",
        "attributes": {
            "Name": {"kind": "string", "value": "Wall A"},
            "OwnerHistory": {"kind": "ref", "id": 10},
        },
        "refs": [
            {"path": "/OwnerHistory", "target": 10, "target_type": "IfcOwnerHistory"},
            {"path": "/ObjectPlacement", "target": 20, "target_type": "IfcLocalPlacement"},
        ],
    }
    out = entity_for_profile(entity, profile="semantic_stable")
    assert out["attributes"] == {"Name": {"kind": "string", "value": "Wall A"}}
    assert out["refs"] == [{"path": "/ObjectPlacement", "target": 20, "target_type": "IfcLocalPlacement"}]


def test_entity_for_profile_semantic_stable_normalizes_owner_history_entity():
    entity = {
        "entity_type": "IfcOwnerHistory",
        "attributes": {"ChangeAction": {"kind": "string", "value": "MODIFIED"}},
        "refs": [{"path": "/OwningUser", "target": 10, "target_type": "IfcPersonAndOrganization"}],
    }
    out = entity_for_profile(entity, profile="semantic_stable")
    assert out == {"entity_type": "IfcOwnerHistory", "attributes": {}, "refs": []}


def test_diff_graphs_rejects_unknown_profile():
    old_graph = {"metadata": {"schema": "IFC4"}, "entities": {}}
    new_graph = {"metadata": {"schema": "IFC4"}, "entities": {}}
    try:
        diff_graphs(old_graph, new_graph, profile="unknown")
        assert False, "expected ValueError for unknown profile"
    except ValueError as exc:
        assert "Unknown profile" in str(exc)


def test_parse_graph_rejects_unknown_profile_before_file_access():
    try:
        parse_graph("missing.ifc", profile="unknown")
        assert False, "expected ValueError for unknown profile"
    except ValueError as exc:
        assert "Unknown profile" in str(exc)
